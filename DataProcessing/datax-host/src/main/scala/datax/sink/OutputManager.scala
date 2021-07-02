// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
package datax.sink

import java.sql.Timestamp

import datax.config.{SettingDictionary, SettingNamespace, SparkEnvVariables}
import datax.constants.{ColumnName, MetricName, ProductConstant}
import datax.data.FileInternal
import datax.exception.EngineException
import datax.fs.HadoopClient
import datax.utility.{DataMerger, DataNormalization, SinkerUtil}
import org.apache.log4j.LogManager
import org.apache.spark.TaskContext
import org.apache.spark.sql.functions.{col, struct, to_json}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


object OutputManager {
  val NamespacePrefix = SettingNamespace.JobOutputPrefix
  val SettingOutputProcessedSchemaPath = "processedschemapath"

  val sinkFactories = Seq[SinkOperatorFactory](
    BlobSinker, EventHubStreamPoster, HttpPoster, CosmosDBSinkerManager, SqlSinker
  ).map(f=>f.getSettingNamespace()->f).toMap

  def getOperatators(dict: SettingDictionary): Seq[OutputOperator] ={
    dict.groupBySubNamespace(NamespacePrefix)
      .map(g=>generateOperator(g._2, g._1))
      .toSeq
  }

  def outputResultReducer = (s1: (Int, Map[String, Int]), s2: (Int, Map[String, Int])) => (s1._1 + s2._1, DataMerger.mergeMapOfCounts(s1._2, s2._2))

  def generateOperator(dict:SettingDictionary, name: String) = {
    val logger = LogManager.getLogger("OutputOperatorsBuilder")
    var flagColumnIndex = 1

    val processedSchemaPath = dict.get(SettingOutputProcessedSchemaPath).orNull
    val sinkOperators = dict
      .groupBySubNamespace()
      .map { case (k, v) => sinkFactories.get(k).map(_.getSinkOperator(v, k))}
      .filter(o => o match {
        case Some(oper) =>
          logger.info(s"Output '$name':${oper.name} is ${SinkerUtil.boolToOnOff(oper.isEnabled)}")
          oper.isEnabled
        case None => false
      }).map(o=>{
      val oper = o.get
      val flagColumnExpr = oper.flagColumnExprGenerator()
      if(flagColumnExpr==null){
        logger.warn(s"Output type:'${oper.name}': no flag column")
        (oper.name, (null, null), oper.generator(-1), oper.onBatch, oper.onInitialization, oper.sinkAsJson)
      }
      else{
        val appendColumn = (flagColumnExpr, s"_${ProductConstant.ProductOutputFilter}_${oper.name}")
        logger.warn(s"Output type:'${oper.name}': append column:$appendColumn")
        flagColumnIndex+=1
        (oper.name, appendColumn, oper.generator(flagColumnIndex), oper.onBatch, oper.onInitialization, oper.sinkAsJson)
      }
    }).toSeq

    if(sinkOperators.length==0)throw new EngineException(s"no sink is defined for output '$name'!")
    logger.warn(s"Output '$name' to ${sinkOperators.length} sinkers: ${sinkOperators.map(s=>s"'${s._1}'").mkString(",")}")

    val flagColumns = sinkOperators.map(_._2).filter(_._1!=null)
    val sinkers = sinkOperators.map(o=>(o._1, o._3, o._6))
    val onBatchHandlers = sinkOperators.map(_._4).filter(_!=null)
    val onInitHandlers = sinkOperators.map(_._5).filter(_!=null)
    var shouldGeneratorProcessedSchema = processedSchemaPath!=null && !processedSchemaPath.isEmpty



    OutputOperator(
      name = name,
      onInitialization = if(onInitHandlers.size>0) (spark: SparkSession)=> for (elem <- onInitHandlers) {elem(spark)} else null,
      onBatch = if(onBatchHandlers.size>0) (spark:SparkSession, time: Timestamp, targets: Set[String]) => {
        onBatchHandlers.foreach(_(spark, time, targets))
      } else null,
      output = (df: DataFrame, partitionTime: Timestamp) => {
        val outputLogger = LogManager.getLogger(s"Output-${name}")
        val outputColumns = df.schema.filterNot(_.name.startsWith(ColumnName.InternalColumnPrefix)).toArray
        if(shouldGeneratorProcessedSchema){
          val spark = df.sparkSession
          spark.synchronized{
            if(shouldGeneratorProcessedSchema){
              HadoopClient.writeHdfsFile(processedSchemaPath, new StructType(outputColumns).prettyJson, true)
              outputLogger.warn(s"Saved processed schema to $processedSchemaPath")
              shouldGeneratorProcessedSchema = false
            }
          }
        }

        val outputColumnNames = outputColumns.map(c=>DataNormalization.sanitizeColumnName(c.name))
        outputLogger.warn(s"Output fields: ${outputColumnNames.mkString(",")}")

        sink(name, df, outputColumnNames, partitionTime, flagColumns, sinkers)
      },
      sinkNames =  sinkOperators.map(o=>(o._1)).toSeq
    )
  }

  def sink(sinkerName:String,
           df: DataFrame,
           outputFieldNames: Seq[String],
           partitionTime: Timestamp,
           flagColumns: Seq[(String, String)],
           outputOperators: Seq[(String, SinkDelegate, Boolean)]) = {

    // Create json'ified dataframe for sinking to outputs that expect json data
    val amendDf = if (df.schema.fieldNames.contains(ColumnName.InternalColumnFileInfo)) df
    else {
      df.withColumn(ColumnName.InternalColumnFileInfo, FileInternal.udfEmptyInternalInfo())
    }

    val jsonDf = amendDf.selectExpr("*" +: flagColumns.map(c => c._1 + " AS " + c._2): _*)
      .select(Seq(col(ColumnName.InternalColumnFileInfo), to_json(struct(outputFieldNames.map(col): _*))) ++
        flagColumns.map(_._2).map(col): _*)


    val count = df.count().toInt
    val inputMetric = Map(s"${MetricName.MetricSinkPrefix}InputEvents" -> count)

    val logger = LogManager.getLogger(s"EventsSinker-${sinkerName}")
    logger.warn(s"Dataframe sinker ${sinkerName} started to sink ${count} events")


    // Get the output operators that sink JSON and non-JSON data and call the output sinkers in parallel
    val jsonOutputOperators = outputOperators.filter(_._3)
    val nonJsonOutputOperators = outputOperators.filter(!_._3)

    if (count > 0) {
      Seq("json", "nonjson").par.map {
        case "json" =>
          if (jsonOutputOperators.size > 0) {
            jsonOutputOperators
              .par
              .map(_._2(jsonDf, partitionTime, sinkerName))
              .reduce(DataMerger.mergeMapOfCounts)
          }
          else {
            Map.empty[String,Int]
          }

        case "nonjson" => {
          if (nonJsonOutputOperators.size > 0) {
            nonJsonOutputOperators
              .par
              .map(_._2(df, partitionTime, sinkerName))
              .reduce(DataMerger.mergeMapOfCounts)
          }
          else {
            Map.empty[String,Int]
          }
        }
      }.reduce(DataMerger.mergeMapOfCounts) ++ inputMetric
    }
    else
      inputMetric
  }
}
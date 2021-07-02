package datax.test.handler

import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}
import com.holdenkarau.spark.testing.{SharedSparkContext, SparkSessionProvider}
import datax.config.SettingDictionary
import datax.extension.DynamicUDF.{Generator1, UDF1}
import datax.extension.PreProjectionProcessor
import datax.fs.HadoopClient
import datax.handler._
import org.apache.hadoop.fs.FileSystemTestHelper.MockFileSystem
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import java.net.URL
import java.io.File
import org.apache.hadoop.fs.{FileSystem, Path}


class TestExtendedUdf extends Generator1[Map[String, String], Map[String, String]]{

  /**
  Initializes a dummy UDF function
    */
  def initialize(spark: SparkSession, dict: SettingDictionary) = {

    UDF1 (
      func = (properties: Map[String, String]) => {
        Map.empty[String,String]
      })
  }
}


class TestPreProjectionProcessor extends PreProjectionProcessor{
  override def initialize(spark: SparkSession, dict: SettingDictionary) = {
    (df: DataFrame) => df
  }
  }

class HandlerTests extends FunSuite with SharedSparkContext with BeforeAndAfter{

  override def conf: SparkConf = super.conf.set(CATALOG_IMPLEMENTATION.key, "hive")


  private def initSparkSession() {
    val builder = SparkSession.builder().enableHiveSupport()
    SparkSessionProvider._sparkSession = builder.getOrCreate()
  }

  before {
    initSparkSession()
  }

  test("test Metrics handler conf generation") {
    // data
    val eh = "eh://kvname/metric-eventhubconnectionstring"
    val redis = "eh://kvname/metric-eventhubconnectionstring"
    val http = "http://localhost/test"

    //settings
    val settingsMap = Map(
      "datax.job.process.metric.eventhub"->eh,
      "datax.job.process.metric.redis"->redis,
      "datax.job.process.metric.httppost"->http
    )

    val settingsDict = SettingDictionary(settingsMap)
    val metricsConf = MetricsHandler.getMetricsSinkConf(settingsDict)

    assert(metricsConf.eventhub === eh)
    assert(metricsConf.redis === redis)
    assert(metricsConf.httpEndpoint === http)
  }



  test("test Azure function handler") {
    //settings
    val settingsMap = Map(
      "datax.job.process.azurefunction.testFunction.serviceendpoint"->"https://mytest.azurewebsites.net",
      "datax.job.process.azurefunction.testFunction.api"->"testApi",
      "datax.job.process.azurefunction.testFunction.code"->"testCode",
      "datax.job.process.azurefunction.testFunction.methodtype"->"post",
      "datax.job.process.azurefunction.testFunction.params"->"p1;p2"
    )

    val settingsDict = SettingDictionary(settingsMap)
    val functions = AzureFunctionHandler.initialize(SparkSessionProvider._sparkSession, settingsDict)
    assert(functions.size===1)
  }


  test("test extended udf handler") {
    //settings
    val settingsMap = Map(
      "datax.job.process.udf.testFunction"->"datax.test.handler.TestExtendedUdf"
    )

    val settingsDict = SettingDictionary(settingsMap)
    val functions = ExtendedUDFHandler.initialize(SparkSessionProvider._sparkSession, settingsDict)
    assert(functions.toMap.size===1)
    assert(functions.toMap.head._1=="testFunction")
  }


  test("test preprojection handler") {
    //settings
    val settingsMap = Map(
      "datax.job.process.preprojection"->"datax.test.handler.TestPreProjectionProcessor"
    )

    val settingsDict = SettingDictionary(settingsMap)
    val dfFunc = PreProjectionHandler.initialize(SparkSessionProvider._sparkSession, settingsDict)
    val rdd = sc.parallelize(Seq(("One", 1), ("Two", 2)))
    val df = SparkSessionProvider._sparkSession.createDataFrame(rdd)
    val result = dfFunc(df)
    assert(result.count()===2)
    println("re="+result.first().getAs[String](0))
    assert(result.first().getAs[String](0)==="One")
  }


  test("test properties handler") {
    //settings
    val settingsMap = Map(
      "datax.job.process.appendproperty.Version"->"12345"
    )

    val settingsDict = SettingDictionary(settingsMap)
    val function = PropertiesHandler.initialize(SparkSessionProvider._sparkSession, settingsDict)

    assert(function.dataType.typeName==="map")
  }


  test("test TimeWindow handler") {
    //settings
    val settingsMap = Map(
      "datax.job.process.timestampcolumn"->"eventTime",
      "datax.job.process.watermark"->"60 second",
      "datax.job.process.timewindow.table1.windowduration"->"5 minutes",
      "datax.job.process.timewindow.table3.windowduration"->"2 minutes"
    )

    val settingsDict = SettingDictionary(settingsMap)
    val timeWindowConf = TimeWindowHandler.initialize(SparkSessionProvider._sparkSession, settingsDict)

    assert(timeWindowConf.isEnabled)
    assert(timeWindowConf.timestampColumn.toString()==="eventTime")
    assert(timeWindowConf.windows.size===2)
    assert(timeWindowConf.maxWindow._1===5)
    assert(timeWindowConf.maxWindow._2===scala.concurrent.duration.MINUTES)
  }

}

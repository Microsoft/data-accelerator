// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
package datax.handler

import java.sql.Timestamp

import datax.config.{SettingDictionary, SettingNamespace}
import datax.constants.ProcessingPropertyName
import datax.data.FileInternal
import datax.utility.DateTimeUtil
import org.apache.log4j.LogManager
import org.apache.spark.SparkEnv
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{Row, SparkSession}

object PropertiesHandler {
  val logger = LogManager.getLogger(this.getClass)
  val SettingPropertyToAppend = "appendproperty"

  private def buildAppendProperties(dict: SettingDictionary, prefix: String):Map[String, String] = {
    dict.getSubDictionary(prefix).getDictMap()
  }

  def initialize(spark: SparkSession, dict: SettingDictionary) = {
    val appendProperties = buildAppendProperties(dict, SettingNamespace.JobProcessPrefix+SettingPropertyToAppend+SettingNamespace.Seperator)
    udf((internalColumn:Row, batchTime:Timestamp) =>
      (appendProperties ++ Map(
        ProcessingPropertyName.BatchTime -> batchTime.toString,
        ProcessingPropertyName.BlobTime -> FileInternal.getInfoFileTimeString(internalColumn),
        ProcessingPropertyName.BlobPathHint -> FileInternal.getInfoOutputFileName(internalColumn),
        ProcessingPropertyName.CPTime -> DateTimeUtil.getCurrentTime().toString,
        ProcessingPropertyName.CPExecutor -> SparkEnv.get.executorId)).filter(_._2!=null))
  }
}

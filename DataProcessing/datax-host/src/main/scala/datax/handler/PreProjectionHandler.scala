// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
package datax.handler

import datax.classloader.ClassLoaderHost
import datax.config.{SettingDictionary, SettingNamespace}
import datax.extension.PreProjectionProcessor
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession

object PreProjectionHandler {
  val logger = LogManager.getLogger(this.getClass)
  val SettingPreProjection = SettingNamespace.JobProcessPrefix + "preprojection"

  def initialize(spark: SparkSession, dict: SettingDictionary) = {
    loadProcessor(spark,
      dict,
      "default",
      dict.get(SettingPreProjection).orNull)
  }

  private def loadProcessor(spark:SparkSession, dict: SettingDictionary, processorName: String, className: String) = {
    if(className==null||className.isEmpty){
      logger.warn(s"no preprojection processor is defined")
      null
    }
    else {
      logger.warn(s"loading class ${className} for preprojection handler '${processorName}'")
      val clazz = ClassLoaderHost.classForName(className)
      val generator = clazz.newInstance().asInstanceOf[PreProjectionProcessor]
      val processor = generator.initialize(spark, dict)
      logger.warn(s"loaded class ${className} for preprojection handler '${processorName}'")
      processor
    }
  }
}

// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
package datax.handler

import datax.config.{SettingDictionary, SettingNamespace}
import datax.exception.EngineException
import datax.securedsetting.KeyVaultClient
import datax.utility.AzureFunctionCaller
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import scala.collection.mutable.ArrayBuffer

object AzureFunctionHandler {

  case class AzureFunctionConf(name: String, serviceEndpoint: String, api: String, code: String, methodType: String, params: Array[String])

  val logger = LogManager.getLogger(this.getClass)
  val SettingAzureFunction = "azurefunction"
  val SettingAzureFunctionServiceEndpoint = "serviceendpoint"
  val SettingAzureFunctionApi = "api"
  val SettingAzureFunctionCode = "code"
  val SettingAzureFunctionMethodType = "methodtype"
  val SettingAzureFunctionParams = "params"

  private def buildAzureFunctionConf(dict: SettingDictionary, name: String): AzureFunctionConf = {
    AzureFunctionConf(
      name = name,
      serviceEndpoint = dict.getOrNull(SettingAzureFunctionServiceEndpoint),
      api = dict.getOrNull(SettingAzureFunctionApi),
      code = KeyVaultClient.resolveSecretIfAny(dict.getOrNull(SettingAzureFunctionCode)),
      methodType = dict.getOrNull(SettingAzureFunctionMethodType),
      // If there are no function params, set it to empty array
      params = dict.getStringSeqOption(SettingAzureFunctionParams).filter(_ != null).map(_.toArray).getOrElse(Array.empty)
    )
  }

  private def buildAzureFunctionConfArray(dict: SettingDictionary, prefix: String): List[AzureFunctionConf] = {
    dict.buildConfigIterable(buildAzureFunctionConf, prefix).toList
  }

  def initialize(spark: SparkSession, dict: SettingDictionary) = {
    var funcs = ArrayBuffer[UserDefinedFunction]()
    val azFuncs = buildAzureFunctionConfArray(dict, SettingNamespace.JobProcessPrefix + SettingAzureFunction + SettingNamespace.Seperator)
    for (azFunc <- azFuncs) {
      val azFuncAccessCode = azFunc.code
      azFunc.params.length match {
        case 0 => funcs += spark.udf.register(azFunc.name, () => AzureFunctionCaller.call(azFunc.serviceEndpoint, azFunc.api, azFuncAccessCode, azFunc.methodType, null))
        case 1 => funcs += spark.udf.register(azFunc.name, (s: String) => AzureFunctionCaller.call(azFunc.serviceEndpoint, azFunc.api, azFuncAccessCode, azFunc.methodType, Map(
          azFunc.params(0) -> s
        )))
        case 2 => funcs += spark.udf.register(azFunc.name, (s1: String, s2: String) => AzureFunctionCaller.call(azFunc.serviceEndpoint, azFunc.api, azFuncAccessCode, azFunc.methodType, Map(
          azFunc.params(0) -> s1,
          azFunc.params(1) -> s2
        )))
        case 3 => funcs += spark.udf.register(azFunc.name, (s1: String, s2: String, s3: String) => AzureFunctionCaller.call(azFunc.serviceEndpoint, azFunc.api, azFuncAccessCode, azFunc.methodType, Map(
          azFunc.params(0) -> s1,
          azFunc.params(1) -> s2,
          azFunc.params(2) -> s3
        )))
        // TODO: Add support for more than 3 input parameters for AzureFunction
        case _ => throw new EngineException("AzureFunction with more than 3 input parameters are currently not supported. Please contact datax dev team for adding support if needed.")
      }
    }
    funcs
  }
}


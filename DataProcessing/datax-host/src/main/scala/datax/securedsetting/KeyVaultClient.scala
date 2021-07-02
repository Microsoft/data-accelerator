// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
package datax.securedsetting

import datax.config.ConfigManager
import datax.constants.JobArgument
import datax.exception.EngineException
import datax.keyvault.KeyVaultMsiAuthenticatorClient
import org.apache.log4j.LogManager
import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import scala.collection.mutable

/***
  * Utility module to access KeyVault service from Azure.
  */
object KeyVaultClient {
  private val logger = LogManager.getLogger(this.getClass)
  private val kvc = KeyVaultMsiAuthenticatorClient.getKeyVaultClient()
  private val cache = new mutable.HashMap[String, String]

  val secretRegex = "^(keyvault|secretscope):\\/\\/([a-zA-Z0-9-_]+)\\/([a-zA-Z0-9-_]+)$".r

  /**
    * get value of a matched secretId from keyvault
    * @param secretId secretId
    * @return value of the secret
    */
  private def resolveSecret(secretId: String): Option[String] = {
    if(secretId==null||secretId.isEmpty)
      return Option(secretId)

    secretRegex.findFirstMatchIn(secretId) match {
      case Some(secretInfo) => val secretType = secretInfo.group(1)
        val vaultName = secretInfo.group(2)
		val secretName = secretInfo.group(3)

        cache.synchronized{
          cache.get(secretId) match {
            case Some(value) => Some(value)
            case None =>
              var value = ""
			  if(secretType == "secretscope"){
				value = dbutils.synchronized{
					dbutils.secrets.get(scope = vaultName, key = secretName)
				}
			  }
			  else{
				value = kvc.synchronized{
				  kvc.getSecret(s"https://$vaultName.vault.azure.net",secretName)
				}.value()
			  }

              logger.warn(s"resolved secret:'$secretId'")
              cache(secretId) = value
              Some(value)
          }
        }

      case None =>
        logger.warn(s"did not resolve:'$secretId', return as is")
        None
    }
  }

  /***
    * get secret from KeyVault with the specified name, could throw exception
    * @param secretId secret uri to retrieve the secret value
    * @return value of the secret
    */
  @throws[EngineException]
  def getSecretOrThrow(secretId: String): String = {
    if(secretId==null || secretId.isEmpty){
      throw new EngineException(s"secret reference cannot be null or empty")
    }
    else{
      resolveSecret(secretId) match {
        case Some(m) => m
        case None => throw new EngineException(s"secret is not found with reference name: '${secretId}'.")
      }
    }
  }

  /***
    * get secret from KeyVault with the specified name, exception handled internally.
    * * @param secretId secret uri to retrieve the secret value
    * * @return value of the secret or None if any exception occurs.
    */
  def getSecret(secretId: String): Option[String] = {
    try{
      Some(getSecretOrThrow(secretId))
    }
    catch {
      case e: Exception =>
        logger.warn(s"skipped '$secretId': ${e.getMessage}")
        None
    }
  }

  /**
    * Try resolve the secretId from input string if there is any
    * a secretId is in format like "keyvault://keyvault-name/secret-name"
    * @param input string with potential secretId in it
    * @return resolve the secret value according to the secretId or return the input string
    */
  def resolveSecretIfAny(input: String): String = {
    resolveSecret(input).getOrElse(input)
  }

  /**
    * Try resolve the secretId from input string if there is any
    * a secretId is in format like "keyvault://keyvault-name/secret-name"
    * @param input string with potential secretId in it
    * @return resolve the secret value according to the secretId or return the input string
    */
  def resolveSecretIfAny(input: Option[String]): Option[String] = {
    input.map(resolveSecretIfAny(_))
  }


  /***
    * a scope to execute operation with the default keyvault name, skip the operation if that doesn't exist.
    * @param callback execution within the scope
    */
  def withKeyVault(callback: (String)=> Unit) = {
    ConfigManager.getActiveDictionary().get(JobArgument.ConfName_DefaultVaultName) match {
      case Some(vaultName) => callback(vaultName)
      case None => logger.warn(s"No default vault is defined, skipped finding key for storage accounts")
    }
  }
}

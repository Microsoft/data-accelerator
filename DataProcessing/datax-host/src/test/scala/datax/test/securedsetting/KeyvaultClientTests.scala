package datax.test.securedsetting

import org.scalatest.FunSuite
import datax.securedsetting.KeyVaultClient
import datax.securedsetting.KeyVaultClient.secretRegex

class KeyvaultClientTests extends FunSuite {

  test("test KeyvaultClient returns input when its not keyvault uri") {
    assert(KeyVaultClient.resolveSecretIfAny("idname")==="idname")
  }

  test("test keyvaultClient's secret regex matches keyvault url") {

    val secretId = "keyvault://kvname/name"

    var secretType=""
    var vaultName=""
    var secretName=""

    KeyVaultClient.secretRegex.findFirstMatchIn(secretId) match {
      case Some(secretInfo) =>
        secretType = secretInfo.group(1)
        vaultName = secretInfo.group(2)
        secretName = secretInfo.group(3)
      case None =>
        None
    }

    assert(secretType==="keyvault")
    assert(vaultName==="kvname")
    assert(secretName==="name")

  }

  test("test keyvaultClient's secret regex matches secretscope url") {

    val secretId = "secretscope://kvname/name"

    var secretType=""
    var vaultName=""
    var secretName=""

    KeyVaultClient.secretRegex.findFirstMatchIn(secretId) match {
      case Some(secretInfo) =>
        secretType = secretInfo.group(1)
        vaultName = secretInfo.group(2)
        secretName = secretInfo.group(3)
      case None =>
        None
    }

    assert(secretType==="secretscope")
    assert(vaultName==="kvname")
    assert(secretName==="name")

  }
}

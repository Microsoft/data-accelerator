package datax.test.config

import datax.config.ConfigManager
import org.scalatest.FunSuite

class configManagerTests extends FunSuite  {

  test("test settingsDictionary creation from arguments") {

    val confFile = "wasbs://dummy@dummyAccount.blob.core.windows.net/test.conf"
    val args = Array("DATAX_CHECKPOINTENABLED=true", "DATAX_DEFAULTVAULTNAME=defaultKV" , "DATAX_DRIVERLOGLEVEL=DEBUG", "conf="+confFile)

    val settingsDict = ConfigManager.getConfigurationFromArguments(args)

    assert(settingsDict.getDictMap().keys.toSeq.intersect(Seq("DATAX_CHECKPOINTENABLED","DATAX_DEFAULTVAULTNAME","DATAX_DRIVERLOGLEVEL","conf", "DATAX_APPCONF")).length===5)
    assert(settingsDict.get("conf").get===confFile)
  }

  test("test settingsDictionary creation throws error if conf is not set") {

    val args = Array("DATAX_CHECKPOINTENABLED=true")
    assertThrows[Error](ConfigManager.getConfigurationFromArguments(args))
  }
}
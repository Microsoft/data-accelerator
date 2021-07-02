package datax.test.sink

import java.net.URI

import com.holdenkarau.spark.testing.SharedSparkContext
import datax.config.SettingDictionary
import datax.fs.HadoopClient
import datax.sink.OutputManager
import org.scalatest.FunSuite
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FilterFileSystem, FileSystem, FSDataInputStream, Path}
import org.apache.hadoop.security.token.Token

class OutputManagerTests extends FunSuite with SharedSparkContext {


  test("test Output Manager") {
    //settings
    val settingsMap = Map(
    "datax.job.output.T1.blob.groupevaluation"->"CASE WHEN DataBucket='main' THEN 'main' ELSE 'other' END",
    "datax.job.output.T1.blob.group.main.folder"->"wasbs://output@test.blob.core.windows.net/json/hourly/%1$tY/%1$tm/%1$td/%1$tH/${quarterBucket}/${minuteBucket}",
    "datax.job.output.T1.blob.group.other.folder"->"wasbs://other@test.blob.core.windows.net/json/hourly/%1$tY/%1$tm/%1$td/%1$tH/${quarterBucket}/${minuteBucket}",

    "datax.job.output.T1.cosmosdb.connectionstring"->"kv://kvname/cosmosOut",
    "datax.job.output.T1.cosmosdb.database"->"outputDb",
    "datax.job.output.T1.cosmosdb.collection"->"outputCol",

    "datax.job.output.T1.sql.connectionstring"->"kv://kvname/sqlout",
    "datax.job.output.T1.sql.databasename"->"sqldb",
    "datax.job.output.T1.sql.table"->"sqlTable",
    "datax.job.output.T1.sql.writemode"->"append",
    "datax.job.output.T1.sql.username"->"kv://kvname/sqlusername",
    "datax.job.output.T1.sql.password"->"kv://kvname/sqlpwd",
    "datax.job.output.T1.sql.url"->"kv://kvname/sqlurl",

    "datax.job.output.T1.eventhub.connectionstring"->"kv://kvname/metricConn",
    "datax.job.output.T1.eventhub.compressiontype"->"none"

    )

    val settingsDict = SettingDictionary(settingsMap)
    val outputs = OutputManager.getOperatators(settingsDict)
    val outputNames = outputs.map(x=>x.name).mkString(",")

    assert(outputs.size===1)
    assert(outputNames.contains("T1"))
    assert(outputs.head.sinkNames.diff(Seq("Blobs","Sql", "CosmosDB","EventHub")).length===0)
    outputs.map(x=>x.output != null).foreach( y => assert(y===true))
  }

}

package datax.test.input

import org.scalatest.FunSuite
import com.holdenkarau.spark.testing.{SharedSparkContext}
import datax.config.SettingDictionary
import datax.input.{BlobPointerInput}
import java.sql.Timestamp

class InputTests extends FunSuite with SharedSparkContext {

  test("test BlobPointerInput.pathsToGroups") {
    // test data
    val blob1 = "wasbs://container@account1.blob.core.windows.net/data/2019-08-14/03/m8beaef7-6f6e-4b6d-9461-a19936089626_20190814_032621.blob"
    val blob2 = "wasbs://container@account1.blob.core.windows.net/data/2019-08-14/03/n8beaef7-6f6e-4b6d-9471-a19936089626_20190814_032621.blob"
    val blob3 = "wasbs://container@account2.blob.core.windows.net/data/2019-08-14/03/p8beaef7-6f6e-4b6d-9481-a19936089626_20190814_032621.blob"

    //settings
    val settingsMap = Map(
      "datax.job.input.default.source"->"account1",
      "datax.job.input.default.source.account1.target"->"account1-out",
      "datax.job.input.default.blobpathregex"->"^wasbs://container@([\\w\\d+]+)\\.blob\\.core\\.windows\\.net/data/\\d{4}-\\d{2}-\\d{2}/\\d{2}/([\\w\\d]{8}-[\\w\\d]{4}-[\\w\\d]{4}-[\\w\\d]{4}-[\\w\\d]{12}_\\d{4}\\d{2}\\d{2}_\\d{2}\\d{2}\\d{2}).blob$",
      "datax.job.input.default.filetimeregex"->"[\\w\\d]{8}-[\\w\\d]{4}-[\\w\\d]{4}-[\\w\\d]{4}-[\\w\\d]{12}_(\\d{4}\\d{2}\\d{2}_\\d{2}\\d{2}\\d{2}).blob$",
      "datax.job.input.default.filetimeformat"->"yyyyMMdd_HHmmss",
      "datax.job.output.default.blob.groupevaluation"->"CASE WHEN DataBucket='main' THEN 'main' ELSE 'other' END",
      "datax.job.output.default.blob.group.main.folder"->"wasbs://output@target.blob.core.windows.net/json/hourly/%1$tY/%1$tm/%1$td/%1$tH/${quarterBucket}/${minuteBucket}",
      "datax.job.output.default.blob.group.other.folder"->"wasbs://other@target.blob.core.windows.net/json/hourly/%1$tY/%1$tm/%1$td/%1$tH/${quarterBucket}/${minuteBucket}"
      )

    val settingsDict = SettingDictionary(settingsMap)

    val list = List(blob1,blob2,blob3)
    val rdd = sc.parallelize(list)

    assert(rdd.count === list.length)

    val groups = BlobPointerInput.pathsToGroups(rdd,"blobpointerInputTest",settingsDict, new Timestamp(System.currentTimeMillis()))
    val matchedGroup = groups.filter(_._1 != null)
    val unMatchedGroup = groups.filter(_._1 == null)

    val matchedBlobs = matchedGroup.head._2.mkString(",")
    val unMatchedBlobs = unMatchedGroup.head._2.mkString(",")

    println("Matched group key:"+ matchedGroup.head._1)
    println("Matched group val:"+ matchedBlobs)
    println("UnMatched group key:"+ unMatchedGroup.head._1)
    println("UnMatched group val:"+ unMatchedBlobs)

    // validate the contents of the matched groups. There should be two groups, one with entries for matched blob account (account1) and other that didn't match
    assert(matchedGroup.head._2.size === 2)
    assert(matchedBlobs.contains(blob1))
    assert(matchedBlobs.contains(blob2))
    assert(matchedBlobs.contains("2019-08-14 03:26:21.0"))
    assert(matchedBlobs.contains("account1-out"))

    assert(unMatchedGroup.head._2.size === 1)
    assert(unMatchedBlobs.contains(blob3))
  }
}

package datax.utility.test

import java.sql.Timestamp

import datax.utility._
import org.scalatest.{FlatSpec, Matchers}
import java.util.zip.GZIPInputStream

class UtilityTests extends FlatSpec with Matchers {

  "DataMerger" should "merge map of strings correctly" in {
    val emptyMap = Map.empty[String, String]
    val mapWithValues1 = Map("Key1"->"Val1", "key2"->"Val2")
    val mapWithValues2 = Map("Key3"->"Val3")

    val emptyMergeResult= DataMerger.mergeMapOfStrings(emptyMap,mapWithValues1)
    assert(emptyMergeResult.keys.size===2)
    assert(emptyMergeResult.toSeq.intersect(Seq("Key1"->"Val1", "key2"->"Val2")).size===2)

    val mergeResult= DataMerger.mergeMapOfStrings(mapWithValues1,mapWithValues2)
    assert(mergeResult.keys.size===3)
    assert(mergeResult.toSeq.intersect(Seq("Key1"->"Val1", "key2"->"Val2", "Key3"->"Val3")).size===3)
  }

  "DataMerger" should "merge map of doubles correctly" in {
    val emptyMap = Map.empty[String, Double]
    val mapWithValues1 = Map("Key1"->1.0, "key2"->2.0)
    val mapWithValues2 = Map("Key3"->3.0)

    val emptyMergeResult= DataMerger.mergeMapOfDoubles(emptyMap,mapWithValues1)
    assert(emptyMergeResult.keys.size===2)
    assert(emptyMergeResult.toSeq.intersect(Seq("Key1"->1.0, "key2"->2.0)).size===2)

    val mergeResult= DataMerger.mergeMapOfDoubles(mapWithValues1,mapWithValues2)
    assert(mergeResult.keys.size===3)
    assert(mergeResult.toSeq.intersect(Seq("Key1"->1.0, "key2"->2.0, "Key3"->3.0)).size===3)
  }


  "DataMerger" should "merge map of counts correctly" in {
    val emptyMap = Map.empty[String, Int]
    val mapWithValues1 = Map("Key1"->1, "key2"->2)
    val mapWithValues2 = Map("Key3"->3)

    val emptyMergeResult= DataMerger.mergeMapOfCounts(emptyMap,mapWithValues1)
    assert(emptyMergeResult.keys.size===2)
    assert(emptyMergeResult.toSeq.intersect(Seq("Key1"->1, "key2"->2)).size===2)

    val mergeResult= DataMerger.mergeMapOfCounts(mapWithValues1,mapWithValues2)
    assert(mergeResult.keys.size===3)
    assert(mergeResult.toSeq.intersect(Seq("Key1"->1, "key2"->2, "Key3"->3)).size===3)
  }

  "DataMerger" should "flatten map of counts correctly" in {
    val mapWithValues1 = Map("Key1"->1, "key2"->2)
    val mapWithValues2 = Map("Key3"->3)

    val mapOfMap = Map("map1"->mapWithValues1, "map2"->mapWithValues2)

    val flattenedMap= DataMerger.flattenMapOfCounts(mapOfMap)
    println("flattened="+flattenedMap.keys.mkString(","))
    assert(flattenedMap.keys.size===3)
    assert(flattenedMap.get("map1_Key1").get===1)
  }

  "DataNormalization" should "add quotes for column names containing - and . correctly" in {
    assert(DataNormalization.sanitizeColumnName("name-a")==="`name-a`")
    assert(DataNormalization.sanitizeColumnName("name.1")==="`name.1`")
  }

  "DateTimeUtil" should "format timestamp value correctly" in {
    val time = Timestamp.valueOf("2019-09-06 17:11:21")
    val result = DateTimeUtil.formatSimple(time)
    println("DateTimeUtil result="+result)
    assert(result==="20190906-171121")
  }

  "StreamingUtility" should "get the ip octate correctly" in {
    val testIp = "255.254.253.2"
    // get the last octate
    val result = StreamingUtility.GetIpOctate(testIp,3)
    println("ip result="+result)
    assert(result===2)
  }

  "MapManipulation lowercaseKeys" should "lowercase the keys correctly" in {
    val mapWithValues = Map("KEY1"->"Val1", "KEY2"->"Val2")
    val result = MapManipulation.lowercaseKeys(mapWithValues)
    assert(result.size===2)
    assert(result.keySet.diff(Set("key1", "key2")).size===0)
  }

  "MapManipulation addProperty" should "lowercase the keys correctly" in {
    val mapWithValues = Map("KEY1"->"Val1", "KEY2"->"Val2")
    val result = MapManipulation.addProperty(mapWithValues,"newKey", "newVal")
    assert(result.size===3)
    assert(result.get("newKey").get==="newVal")
  }

  "GZipHelper" should "deflate string correctly" in {

   val input = "hello world"
   val gzipResult = GZipHelper.deflate(input)
   assert(gzipResult!=input)
   val unzipped = GZipHelper.inflate(gzipResult)
   assert(unzipped===input)
  }

  "GZipHelper" should "deflate to byte array correctly" in {

    val input = "hello world"
    val gzipResult = GZipHelper.deflateToBytes(input)

    // As per the GZip spec, the first two bytes for gzipped array contains GZIP_MAGIC magic number, check for that
    val gzipMagicNumber = (gzipResult(0).asInstanceOf[Int] & 0xff) | ((gzipResult(1) << 8) & 0xff00)
    assert(GZIPInputStream.GZIP_MAGIC === gzipMagicNumber)

    val unzipped = GZipHelper.inflateBytes(gzipResult)
    assert(unzipped===input)
  }

}

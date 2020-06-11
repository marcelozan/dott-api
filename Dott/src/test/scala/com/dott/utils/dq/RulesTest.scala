package com.dott.utils.dq

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{Column, Row, SparkSession, functions}
import org.scalatest.{FlatSpec, Matchers, PrivateMethodTester}
import testutils.{DataFrameUtils, SparkSessionProvider}
import org.apache.spark.sql.functions._

class RulesTest extends FlatSpec with Matchers with SparkSessionProvider with DataFrameUtils with PrivateMethodTester {
  override def sparkConf: SparkConf = {
    new SparkConf().
      setMaster("local").
      setAppName("test").
      set("spark.ui.enabled", "false").
      set("spark.app.id", appID).
      set("spark.driver.host", "localhost")
  }
  val spark  = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()
  import spark.implicits._

  "duplicate" should "return true for the last row with the same key and false to the others" in {
    val input =
      Seq(
        Row("key1", 1),
        Row("key1", 2),
        Row("key1", 3)
      )
    val schema = StructType(Seq(StructField("key", StringType, true),StructField ("order", IntegerType, true)))
    val df = create(input, schema, spark).withColumn("dup", Rules.duplicate($"key", $"order"))
    val result1 = df.filter($"dup").collect().map(r => r.getInt(1)).head
    val result2 = df.filter($"dup".unary_!).count()
    result1 shouldBe 3
    result2 shouldBe 2

  }

  "duplicate" should "return true for all rows if no duplications found" in {
    val input =
      Seq(
        Row("key1", 1),
        Row("key2", 2),
        Row("key3", 3)
      )
    val schema = StructType(Seq(StructField("key", StringType, true),StructField ("order", IntegerType, true)))
    val df = create(input, schema, spark).withColumn("dup", Rules.duplicate($"key", $"order"))
    val result1 = df.filter($"dup").count()
    result1 shouldBe 3


  }

  "notNullCol" should "return true when col is not null" in {
    val input =
      Seq(
        Row("test")
      )
    val schema = StructType(Seq(StructField("key", StringType, true)))
    val df = create(input, schema, spark).select(Rules.notNullCol($"key").as("result"))
    val result1 = df.filter($"result").count()
    result1 shouldBe 1

  }
  "notNullCol" should "return false when col is null" in {
    val input =
      Seq(
        Row(null)
      )
    val schema = StructType(Seq(StructField("key", StringType, true)))
    val df = create(input, schema, spark).select(Rules.notNullCol($"key").as("result"))
    val result1 = df.filter($"result".unary_!).count()
    result1 shouldBe 1
  }

  "validLat" should "return false when col is an invalid latitude" in {
    val input =
      Seq(
        Row(90.1),
        Row(-90.1)
      )
    val schema = StructType(Seq(StructField("input", DoubleType, true)))
    val df = create(input, schema, spark).select(GeoCoordRules.validLat($"input").as("result"))
    val result1 = df.filter($"result".unary_!).count()
    result1 shouldBe 2
  }
  "validLat" should "return true when col is an valid latitude" in {
    val input =
      Seq(
        Row(90.0),
        Row(-90.0)
      )
    val schema = StructType(Seq(StructField("input", DoubleType, true)))
    val df = create(input, schema, spark).select(GeoCoordRules.validLat($"input").as("result"))
    val result1 = df.filter($"result").count()
    result1 shouldBe 2
  }
  "validLon" should "return false when col is an invalid longitude" in {
    val input =
      Seq(
        Row(180.1),
        Row(-180.1)
      )
    val schema = StructType(Seq(StructField("input", DoubleType, true)))
    val df = create(input, schema, spark).select(GeoCoordRules.validLon($"input").as("result"))
    val result1 = df.filter($"result".unary_!).count()
    result1 shouldBe 2
  }
  "validLon" should "return true when col is an valid longitude" in {
    val input =
      Seq(
        Row(180.0),
        Row(180.0)
      )
    val schema = StructType(Seq(StructField("input", DoubleType, true)))
    val df = create(input, schema, spark).select(GeoCoordRules.validLon($"input").as("result"))
    val result1 = df.filter($"result").count()
    result1 shouldBe 2
  }
  "startEndTimes" should "return true when start date is less than or equal end date" in {
    import java.sql.Timestamp
    val input =
      Seq(
        Row(Timestamp.valueOf("2019-05-02 19:30:43.433"), Timestamp.valueOf("2019-05-02 19:30:43.434")),
          Row(Timestamp.valueOf("2019-05-02 19:30:43.434"), Timestamp.valueOf("2019-05-02 19:30:43.434"))
      )
    val schema = StructType(Seq(StructField("start", TimestampType, true),StructField("end", TimestampType, true)))
    val df = create(input, schema, spark).select(TimeRules.startEndTimes($"start", $"end").as("result"))
    val result1 = df.filter($"result").count()
    result1 shouldBe 2
  }
  "startEndTimes" should "return false when start date is less than end date" in {
    import java.sql.Timestamp
    val input =
      Seq(
        Row(Timestamp.valueOf("2019-05-02 19:30:43.434"), Timestamp.valueOf("2019-05-02 19:30:43.433"))
      )
    val schema = StructType(Seq(StructField("start", TimestampType, true),StructField("end", TimestampType, true)))
    val df = create(input, schema, spark).select(TimeRules.startEndTimes($"start", $"end").as("result"))
    val result1 = df.filter($"result".unary_!).count()
    result1 shouldBe 1
  }

  "reasons" should "return null if the test is successful" in {
    val r = Rule("greater than 0", $"input" > 0)
    val reason = PrivateMethod[Column]('reason)
    val input =
      Seq(
        Row(1)
      )
    val schema = StructType(Seq(StructField("input", IntegerType, true)))
    val df = create(input, schema, spark).select((Builder invokePrivate reason(r)).as("result"))
    val result1 = df.filter($"result".isNull).count()
    result1 shouldBe 1
  }
  "reasons" should "return the test's name if the test fails" in {
    val r = Rule("greater than 0", $"input" > 0)
    val reason = PrivateMethod[Column]('reason)
    val input =
      Seq(
        Row(0)
      )
    val schema = StructType(Seq(StructField("input", IntegerType, true)))
    val df = create(input, schema, spark).select((Builder invokePrivate reason(r)).as("result"))
    val result1 = df.filter($"result" === r.name).count()
    result1 shouldBe 1
  }
  "arrayBuild" should "add an array to the DataFrame with only failed tests." in {
    val rs = Seq(Rule("greater than 0", $"input" > 0),
      Rule("equals to 0", $"input" === 0))
    val input =
      Seq(
        Row(0)
      )
    val schema = StructType(Seq(StructField("input", IntegerType, true)))
    val df = create(input, schema, spark)
    val dfArray = Builder.arrayBuild(df, rs, "checks")
    val result1 = dfArray.filter(functions.size($"checks") === 1).count()
    result1 shouldBe 1
    val result2 = dfArray.filter(array_contains($"checks", rs.head.name)).count()
    result2 shouldBe 1
  }











}

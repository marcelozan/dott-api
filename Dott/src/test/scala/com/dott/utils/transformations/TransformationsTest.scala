package com.dott.utils.transformations

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.{FlatSpec, Matchers, PrivateMethodTester}
import testutils.{DataFrameUtils, SparkSessionProvider}
import org.apache.spark.sql.functions._

class TransformationsTest extends FlatSpec with Matchers with SparkSessionProvider with DataFrameUtils with PrivateMethodTester{

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

  "diffInSeconds" should "return the difference in seconds between to Timestamps" in {
    import java.sql.Timestamp
    val input =
      Seq(
        Row(Timestamp.valueOf("2019-05-02 19:30:40.433"), Timestamp.valueOf("2019-05-02 19:30:43.434")),
        Row(Timestamp.valueOf("2019-05-02 19:30:43.433"), Timestamp.valueOf("2019-05-02 19:30:43.434"))
      )
    val schema = StructType(Seq(StructField("start", TimestampType, true),StructField("end", TimestampType, true)))
    val df = create(input, schema, spark).select(TimestampTransformations.diffInSeconds($"start",$"end" ).as("result"))
    val result1 = df.filter($"result" === 3).count()
    val result2 = df.filter($"result" === 0).count()
    result1 shouldBe 1
    result2 shouldBe 1
  }
  "distance" should "distance in meters between to points" in {

    val input =
      Seq(
        Row(48.829338073730469,2.3760232925415039,48.83233642578125,2.3605866432189941)

      )
    val schema = StructType(
      Seq(
        StructField("startLat", DoubleType, true)
        ,StructField("startLon", DoubleType, true)
        ,StructField("endLat", DoubleType, true)
        ,StructField("endtLon", DoubleType, true)
      )
    )

    val df = create(input, schema, spark)
      .select(GeoTransformations.distance(
        $"startLat",
        $"startLon",
        $"endLat",
        $"endtLon" ).as("result"))
    val result =df.filter(col("result").cast(IntegerType) === 1178).count()
    result shouldBe 1
  }





}

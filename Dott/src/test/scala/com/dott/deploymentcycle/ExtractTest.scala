package com.dott.deploymentcycle

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.{FlatSpec, Matchers}
import testutils.{DataFrameUtils, SparkSessionProvider}

class ExtractTest extends FlatSpec with Matchers with SparkSessionProvider with DataFrameUtils {

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
  "cleanTimestamp" should "remove the UTC and spaces from the begging and and of a column" in {
    val input =
      Seq(
        Row("2019-05-02 19:30:43.433 UTC")
      )
    val schema = StructType(Seq(StructField("input", StringType, true)))
    val df = create(input, schema, spark).select(Extract.cleanTimestamp($"input").as("input1"))
    val result = df.filter($"input1" === "2019-05-02 19:30:43.433").count()
    result shouldBe 1

  }
  "cleanTimestamp" should "return the input as is, if no spaces or UTC is found" in {
    val input =
      Seq(
        Row("2019-05-02 19:30:43.433")
      )
    val schema = StructType(Seq(StructField("input", StringType, true)))
    val df = create(input, schema, spark).select(Extract.cleanTimestamp($"input").as("input1"))
    val result = df.filter($"input1" === "2019-05-02 19:30:43.433").count()
    result shouldBe 1

  }

  "read" should "return a dataFrame with the same number of rows from the source file, minus the header" in {
    val df = Extract.read(Extract.ridesFileName, Extract.ridesSchema)
    df.count() shouldBe 79042

  }

  "read" should "return a dataFrame with the schema passed as parameter" in {
    val df = Extract.read(Extract.ridesFileName, Extract.ridesSchema)
    df.schema shouldBe Extract.ridesSchema

  }




}
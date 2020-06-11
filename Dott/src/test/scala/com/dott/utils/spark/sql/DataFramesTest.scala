package com.dott.utils.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Column, Row, SparkSession}
import org.scalatest.{FlatSpec, Matchers, PrivateMethodTester}
import testutils.{DataFrameUtils, SparkSessionProvider}
import org.apache.spark.sql.functions._

class DataFramesTest  extends FlatSpec with Matchers with SparkSessionProvider with DataFrameUtils with PrivateMethodTester {
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

  "renameCols" should "return a dataframe with the cols mapped as in Map" in {
    val input =
      Seq(
        Row("key1", 1),
        Row("key1", 2),
        Row("key1", 3)
      )
    val schema = StructType(Seq(StructField("key", StringType, true),StructField ("order", IntegerType, true)))
    val map = Map("key" -> "key1", "order" -> "order1")
    val df = create(input, schema, spark)
    val dfResult = DataFrames.renameCols(df, map)
    val result = dfResult.columns
    result shouldBe map.values.toArray

  }

  "unionDataFrames" should "return a single dataframe resulting from the union of two or more dataframes" in {
    val input1 =
      Seq(
        Row("key1", 1),
        Row("key1", 2),
        Row("key1", 3)
      )
    val input2 =
      Seq(
        Row("key4", 1),
        Row("key5", 2),
        Row("key6", 3)
      )
    val input3 =
      Seq(
        Row("key7", 1),
        Row("key8", 2),
        Row("key9", 3)
      )
    val schema = StructType(Seq(StructField("key", StringType, true),StructField ("order", IntegerType, true)))
    val map = Map("key" -> "key1", "order" -> "order1")
    val dfs = Seq(create(input1, schema, spark), create(input2, schema, spark), create(input3, schema, spark))
    val dfResult = DataFrames.unionDataFrames(dfs)
    val result = dfResult.count()
    result shouldBe 9
  }

  "conformDataFrame" should "return a new dataframe with all the column in colsToConform" in {
    val input =
      Seq(
        Row("key1", 1),
        Row("key1", 2),
        Row("key1", 3)
      )

    val schema = StructType(Seq(StructField("key", StringType, true),StructField ("order", IntegerType, true)))
    val colsToConform : Seq[String] = ("new" :: "key" :: "order" :: Nil)
    val df = create(input, schema, spark)
    val dfResult = DataFrames.conformDataFrame(df, colsToConform)
    val result = dfResult.columns
    result.sorted shouldBe colsToConform.toArray.sorted
  }

  "addTransformations" should "return a new dataframe with the column added" in {
    val input = Seq(
      Row(1)
    )
    val schema = StructType(Seq(StructField("input", IntegerType, true)))
    val df = create(input, schema, spark)
    val tranformations:  Map[String, Column] = Map("plus1" -> col("input").+(1)
    ,"times5" -> col("input").*(5))
    val result = DataFrames.addTransformations(df, tranformations).filter($"plus1"=== 2 && $"times5" === 5)
    result.count() shouldBe 1
  }

  "castSchema" should "return a new dataframe with the columns casted and renamed by the new schema" in {
    val input = Seq(
      Row(1)
    )
    val schema = StructType(Seq(StructField("input", StringType, true)))
    val newSchema = StructType(Seq(StructField("input", IntegerType, true)))
    val df = create(input, schema, spark)
    val result = DataFrames.castSchema(df, newSchema)
    result.schema shouldBe newSchema
  }



}

package com.dott.utils.storage

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, Matchers}
import testutils.SparkSessionProvider


class ReadTest extends FlatSpec with Matchers with SparkSessionProvider {

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

  val reader = new Read
  val path = this.getClass.getClassLoader.getResource("test_read").getPath().toString

  "readCsv" should "return the number of rows from the file" in {
    val df = reader.readCSV(path)
    df.count() shouldBe 3
  }

  "readCsv" should "return the number of columns from the file" in {
    val df = reader.readCSV(path)
    df.columns.size shouldBe 2
  }



}

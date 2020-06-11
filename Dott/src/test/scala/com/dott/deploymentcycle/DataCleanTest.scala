package com.dott.deploymentcycle

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession, functions}
import org.scalatest.{FlatSpec, Matchers}
import testutils.{DataFrameUtils, SparkSessionProvider}
import java.sql.Timestamp

import org.apache.spark.sql.functions._

class DataCleanTest  extends FlatSpec with Matchers  with SparkSessionProvider with DataFrameUtils  {
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
  "dataQualityDeployments" should "return same number of rows of input if all the tests are sucessful" in {

    val input = Seq("6fy0fO6BHW2IvxunS54N","i1Fycyzyi1HeeEN8eVh1",Timestamp.valueOf("2019-05-19 02:34:31.999"),Timestamp.valueOf("2019-05-21 04:52:29.113"))
    val r = Seq(Row.fromSeq(input))
    val df = create(r, Extract.deploymentsSchema, spark)
    val result = DataClean.dataQualityDeployments(df).count()
    result shouldBe r.size
  }
  "dataQualityDeployments" should "exclude the row if all values in a row are null" in {

    val input = Seq(null, null, null, null)
    val r = Seq(Row.fromSeq(input))
    val df = create(r, Extract.deploymentsSchema, spark)
    val result = DataClean.dataQualityDeployments(df).count()

    result shouldBe 0
  }
  "dataQualityDeployments" should "return one row from duplicates rows" in {

    val input = Seq.fill(2)(
      Seq("yV4kXBtAx6GNRcJDLsF2","JkDkmUHoZ4ngg7hAx0F4", Timestamp.valueOf("2019-05-19 02:34:31.999"),Timestamp.valueOf("2019-05-21 04:52:29.113"))
    )

    val r = input.map(s => Row.fromSeq(s))
    val df = create(r, Extract.deploymentsSchema, spark)
    val result = DataClean.dataQualityDeployments(df).count()

    result shouldBe r.size - 1
  }
  "dataQualityPickups" should "return the same number of rows from input if all the tests are sucessful" in {

    val input = Seq("yV4kXBtAx6GNRcJDLsF2","JkDkmUHoZ4ngg7hAx0F4","XXBGC5", Timestamp.valueOf("2019-05-19 02:34:31.999"),Timestamp.valueOf("2019-05-21 04:52:29.113"))
    val r = Seq(Row.fromSeq(input))
    val df = create(r, Extract.pickupsSchema, spark)
    val result = DataClean.dataQualityPickups(df).count()
    result shouldBe r.size
  }
  "dataQualityPickups" should "exclude the row if all values in a row are null" in {

    val input = Seq(null, null, null, null, null)
    val r = Seq(Row.fromSeq(input))
    val df = create(r, Extract.pickupsSchema, spark)
    val result = DataClean.dataQualityPickups(df).count()

    result shouldBe 0
  }
  "dataQualityPickups" should "return one row from duplicates rows" in {

    val input = Seq.fill(2)(
      Seq("yV4kXBtAx6GNRcJDLsF2","JkDkmUHoZ4ngg7hAx0F4","XXBGC5", Timestamp.valueOf("2019-05-19 02:34:31.999"),Timestamp.valueOf("2019-05-21 04:52:29.113"))
    )
    val r = input.map(s => Row.fromSeq(s))
    val df = create(r, Extract.pickupsSchema, spark)
    val result = DataClean.dataQualityPickups(df).count()

    result shouldBe r.size - 1
  }

  "dataQualityRides" should "return the same number of rows from input if all the tests are sucessful" in {
    val input = Seq("dVPK7fEkGfd3HJiYRMkj",
      "f42vVrTOJh7yVYJACDCv",
      Timestamp.valueOf("2019-05-19 02:34:31.999"),
      Timestamp.valueOf("2019-05-21 04:52:29.113"),
      48.829338073730469,
      2.3760232925415039,
      48.83233642578125,
      2.3605866432189941,
      2.0
    )
    val r = Seq(Row.fromSeq(input))
    val df = create(r, Extract.ridesSchema, spark)
    val result = DataClean.dataQualityRides(df).count()
    result shouldBe r.size
  }
  "dataQualityRides" should "exclude the row if all values in a row are null" in {

    val input = Seq.fill(9)(null)
    val r = Seq(Row.fromSeq(input))
    val df = create(r, Extract.ridesSchema, spark)
    val result = DataClean.dataQualityRides(df).count()

    result shouldBe 0
  }
  "dataQualityRides" should "return one row from duplicates rows" in {

    val input = Seq.fill(2)(
      Seq("dVPK7fEkGfd3HJiYRMkj",
        "f42vVrTOJh7yVYJACDCv",
        Timestamp.valueOf("2019-05-19 02:34:31.999"),
        Timestamp.valueOf("2019-05-21 04:52:29.113"),
        48.829338073730469,
        2.3760232925415039,
        48.83233642578125,
        2.3605866432189941,
        2.0
      )
    )
    val r = input.map(s => Row.fromSeq(s))
    val df = create(r, Extract.ridesSchema, spark)
    val result = DataClean.dataQualityRides(df).count()

    result shouldBe r.size - 1
  }


}

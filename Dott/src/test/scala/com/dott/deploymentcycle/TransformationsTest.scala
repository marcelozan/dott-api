package com.dott.deploymentcycle

import java.sql.Timestamp

import com.dott.utils.spark.sql.DataFrames
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession, functions}
import org.scalatest.{FlatSpec, Matchers, PrivateMethodTester}
import testutils.{DataFrameUtils, SparkSessionProvider}

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
  val inputDeployments = Seq("6fy0fO6BHW2IvxunS54N"
    ,"i1Fycyzyi1HeeEN8eVh1"
    ,Timestamp.valueOf("2019-05-19 02:34:31.999")
    ,Timestamp.valueOf("2019-05-21 04:52:29.113"))

  val inputPickups = Seq("6fy0fO6BHW2IvxunS55N"
    ,"i1Fycyzyi1HeeEN8eVh1"
    ,"XXBGC5"
    ,Timestamp.valueOf("2019-05-23 02:34:31.999")
    ,Timestamp.valueOf("2019-05-24 04:52:29.113"))

  val inputRides = Seq("dVPK7fEkGfd3HJiYRMkj",
    "i1Fycyzyi1HeeEN8eVh1",
    Timestamp.valueOf("2019-05-22 02:34:31.999"),
    Timestamp.valueOf("2019-05-22 04:52:29.113"),
    48.829338073730469,
    2.3760232925415039,
    48.83233642578125,
    2.3605866432189941,
    2.0
  )


  "addConfCols" should "should add the event_timestamp = time_cycle_started and event_type = deployments for deployments data" in {
    val r = Seq(Row.fromSeq(inputDeployments))
    val df = create(r, Extract.deploymentsSchema, spark)
    val renamed = DataFrames.renameCols(df, Transformations.renameDeploymentsCols)
    val result = Transformations.addConfCols(renamed, "deployments")
      .filter($"time_cycle_started" ===$"event_time" && $"event_type" === "deployments").count()
    result shouldBe 1
  }
  "addConfCols" should "should add the event_timestamp = time_cycle_ended and event_type = pickups for pickups data" in {
    val r = Seq(Row.fromSeq(inputPickups))
    val df = create(r, Extract.pickupsSchema, spark)
    val renamed = DataFrames.renameCols(df, Transformations.renamePickupsCols)
    val result = Transformations.addConfCols(renamed, "pickups")
      .filter($"time_cycle_ended" ===$"event_time" && $"event_type" === "pickups").count()
    result shouldBe 1
  }
  "addConfCols" should "should add the event_timestamp = time_ride_started and event_type = rides for rides data" in {
    val r = Seq(Row.fromSeq(inputRides))
    val df = create(r, Extract.ridesSchema, spark)
    val renamed = DataFrames.renameCols(df, Transformations.renameRidesCols)
    val result = Transformations.addConfCols(renamed, "rides")
      .filter($"time_ride_started" ===$"event_time" && $"event_type" === "rides").count()
    result shouldBe 1
  }
}

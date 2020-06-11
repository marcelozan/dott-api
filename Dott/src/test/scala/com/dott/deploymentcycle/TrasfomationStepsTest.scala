package com.dott.deploymentcycle

import com.dott.deploymentcycle.Transformations.{addConfCols, aggregateMetrics, buidCycle, cleanAnomalies, renameDeploymentsCols, renamePickupsCols, renameRidesCols}
import com.dott.utils.spark.sql.DataFrames
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions
import org.scalatest.{FlatSpec, Matchers, PrivateMethodTester}
import testutils.{DataFrameUtils, SparkSessionProvider}

/**
 * This class should test the transformations, not as unit, but doing some checks at important metrics like
 * duplicateds
 * row counts
 * important metrics
 */
class TrasfomationStepsTest extends FlatSpec with Matchers with SparkSessionProvider with DataFrameUtils with PrivateMethodTester {
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

  /**
   * Data Preparation
   * will generate the intermidiate dataframes for the test
   * to execute those tests, the source files must be in the path configured at ApplicationParameters
   */
  val extractDep = Extract.read(Extract.deploymentsFileName, Extract.deploymentsSchema)
  val extractPic = Extract.read(Extract.pickupsFileName, Extract.pickupsSchema)
  val extractRid = Extract.read(Extract.ridesFileName, Extract.ridesSchema)

  val cleanDep = DataClean.dataQualityDeployments(extractDep)
  val cleanPic = DataClean.dataQualityPickups(extractPic)
  val cleanRid = DataClean.dataQualityRides(extractRid)

  val deploymentsRen = DataFrames.renameCols(cleanDep, renameDeploymentsCols)
  val pickupsRen = DataFrames.renameCols(cleanPic, renamePickupsCols)
  val ridesRen = DataFrames.renameCols(cleanRid, renameRidesCols)

  val ridesCalc = DataFrames.addTransformations(ridesRen, RidesTransformations.columnTransformations)

  val depColAdded = addConfCols(deploymentsRen, "deployments")
  val picColAdded = addConfCols(pickupsRen, "pickups")
  val ridColAdded = addConfCols(ridesCalc, "rides")

  val union = DataFrames.mergeDataFrames(depColAdded :: picColAdded :: ridColAdded :: Nil)


  val cycleBuild = buidCycle(union)
  val noAnomalies = cleanAnomalies(cycleBuild)

  val cycletMetrcis = aggregateMetrics(noAnomalies)


  val cycletMetrcisCalc = DataFrames.addTransformations(cycletMetrcis, CycleTransformations.columnTransformations)

  val ridesOutput = noAnomalies.filter(col("event_type") === "rides")
    .select(RidesTransformations.rideOutputColumns.head, RidesTransformations.rideOutputColumns.tail: _*)

  "union" should "have the same number of rows the three cleaned " in {
    val cleaned = cleanRid.count() + cleanDep.count() + cleanPic.count()

    union.count() shouldBe cleaned
  }

  "noAnomalies" should "have no rows with deployment_id null" in {
    noAnomalies.filter($"deployment_id".isNull).count() shouldBe 0
  }

  "noAnomalies" should "not have deployment_id with more than 1 pickup_id" in {
    val a = noAnomalies.groupBy("deployment_id")
        .agg(countDistinct("pickup_id").as("test"))
        .filter($"test" > 1)
     a.count() shouldBe 0
  }

  "noAnomalies" should "not have pickup_id(non null) with more than 1 deployment_id" in {
    val a = noAnomalies.filter($"pickup_id".isNotNull)
      .groupBy("pickup_id")
      .agg(countDistinct("deployment_id").as("test"))
      .filter($"test" > 1)
    a.count() shouldBe 0
  }
  "noAnomalies" should "not have rides after a pickup" in {
    val window = Window.partitionBy("vehicle_id").orderBy("event_time")
    val a = noAnomalies
        .withColumn("previous_event", lag("event_time", 1) over window)
      .filter($"event_type" === "rides" && $"previous_event" === "pickups")
    a.count() shouldBe 0
  }
  "cycletMetrcis" should "have the same row count of distinct deployment_id on noAnomalies" in {
    val d = noAnomalies.select("deployment_id").distinct()



    cycletMetrcis.count() shouldBe d.count()
  }
  "cycletMetrcisCalc" should "have no negative cycle_duration" in {
    cycletMetrcisCalc.filter($"cycle_duration" < 0).count() shouldBe 0
  }








}

package com.dott.deploymentcycle

import com.dott.utils.dq._
import com.dott.utils.storage.Write
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions

object DataClean extends Write with ApplicationParameters {
  val spark = SparkSession.builder().getOrCreate()

  import spark.implicits._

  /**
   * The list of checks done on deployments dataframe
   */
  val deploymentsDQRules = Seq(
    Rule("Null value on task_id", Rules.notNullCol($"task_id"))
    , Rule("Null value on vehicle_id", Rules.notNullCol($"vehicle_id"))
    , Rule("Null value on time_task_created", Rules.notNullCol($"time_task_created"))
    , Rule("Null value on time_task_resolved", Rules.notNullCol($"time_task_resolved"))
    , Rule("time_task_created is not smaller than or equal to time_task_resolved", TimeRules.startEndTimes($"time_task_created", $"time_task_resolved"))
    , Rule("duplicated task_id", Rules.duplicate($"task_id", $"time_task_created"))
  )

  /**
   * The list of checks done on pickups dataframe
   */

  val pickupsDQRules = Seq(
    Rule("Null value on task_id", Rules.notNullCol($"task_id"))
    , Rule("Null value on vehicle_id", Rules.notNullCol($"vehicle_id"))
    , Rule("Null value on qr_code", Rules.notNullCol($"qr_code"))
    , Rule("Null value on time_task_created", Rules.notNullCol($"time_task_created"))
    , Rule("Null value on time_task_resolved", Rules.notNullCol($"time_task_resolved"))
    , Rule("time_task_created is not smaller than or equal to  time_task_resolved", TimeRules.startEndTimes($"time_task_created", $"time_task_resolved"))
    , Rule("duplicated task_id", Rules.duplicate($"task_id", $"time_task_created"))
  )

  /**
   * The list of checks done on rides dataframe
   */
  val ridesDQRules = Seq(
    Rule("Null value on ride_id", Rules.notNullCol($"ride_id"))
    , Rule("Null value on vehicle_id", Rules.notNullCol($"vehicle_id"))
    , Rule("Null value on time_ride_start", Rules.notNullCol($"time_ride_start"))
    , Rule("Null value on time_ride_end", Rules.notNullCol($"time_ride_end"))
    , Rule("Null value on start_lat", Rules.notNullCol($"start_lat"))
    , Rule("Null value on start_lng", Rules.notNullCol($"start_lng"))
    , Rule("Null value on end_lat", Rules.notNullCol($"end_lat"))
    , Rule("Null value on end_lng", Rules.notNullCol($"end_lng"))
    , Rule("Null value on gross_amount", Rules.notNullCol($"gross_amount"))
    , Rule("time_ride_start is not smaller than or equal to  time_ride_end", TimeRules.startEndTimes($"time_ride_start", $"time_ride_end"))
    , Rule("Invalid start_lat", GeoCoordRules.validLat($"start_lat"))
    , Rule("Invalid end_lat", GeoCoordRules.validLat($"end_lat"))
    , Rule("Invalid start_lng", GeoCoordRules.validLon($"start_lng"))
    , Rule("Invalid end_lng", GeoCoordRules.validLon($"end_lng"))
    , Rule("Invalid gross_amount", $"gross_amount" >= 0.0)
    , Rule("duplicated ride_id", Rules.duplicate($"ride_id", $"time_ride_start"))
  )

  /**
   *
   * @param df raw Deployments df
   * @return returns the deployments with the quality checks array column
   */
  def dataQualityDeployments(df: DataFrame): DataFrame = {
    val input = Builder.arrayBuild(df, deploymentsDQRules, "quality_checks")
    val ret = writeCleanResults(input, "quality_checks", rejectDeploymentsFolder)
    ret
  }

  /**
   *
   * @param df raw Pickups df
   * @return returns the pickups with the quality checks array column
   */
  def dataQualityPickups(df: DataFrame): DataFrame = {
    val input = Builder.arrayBuild(df, pickupsDQRules, "quality_checks")
    val ret = writeCleanResults(input, "quality_checks", rejectPickupsFolder)
    ret
  }
  /**
   *
   * @param df raw Rides df
   * @return returns the Rides with the quality checks array column
   */
  def dataQualityRides(df: DataFrame): DataFrame = {
    val input = Builder.arrayBuild(df, ridesDQRules, "quality_checks")
    val ret = writeCleanResults(input, "quality_checks", rejectRidesFolder)
    ret
  }

  def writeCleanResults(df: DataFrame, col: String, path: String) : DataFrame = {
    val dfPer = df.persist(newLevel = StorageLevel.MEMORY_AND_DISK_SER)
    writeJson(dfPer.filter(functions.size(functions.col(col)) > 0), path)
    val ret = dfPer.filter(functions.size(functions.col(col)) === 0).drop(col)
    ret
  }



}

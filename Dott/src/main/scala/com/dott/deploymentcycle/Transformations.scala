package com.dott.deploymentcycle

import com.dott.utils.spark.sql.DataFrames
import com.dott.utils.transformations._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._


object Transformations {

  //cols to be renamed, due business conformation
  val renameDeploymentsCols = Map("task_id" -> "deployment_id"
    , "time_task_created" -> "time_deployment_created"
    , "time_task_resolved" -> "time_cycle_started")

  val renamePickupsCols = Map("task_id" -> "pickup_id"
    , "time_task_created" -> "time_cycle_ended"
    , "time_task_resolved" -> "time_pickup_done")

  val renameRidesCols = Map("time_ride_start" -> "time_ride_started"
    , "time_ride_end" -> "time_ride_ended"
    , "start_lat" -> "start_latitude"
    , "end_lat" -> "end_latitude"
    , "start_lng" -> "start_longitude"
    , "end_lng" -> "end_longitude"
  )

  //defition of event time for each type of event
  val eventTimeColumns = Map("deployments" -> "time_cycle_started"
    , "pickups" -> "time_cycle_ended"
    , "rides" -> "time_ride_started")

  /**
   *
   * @param df     Dataframe which the colums must be added
   * @param dfType the Dataframe type, can be deployments, pickups, rides
   * @return a Dataframe with the event_tme (see the mapping eventTimeColumns) and the type of the DataFrame
   */

  def addConfCols(df: DataFrame, dfType: String): DataFrame = {
    val eventTime = eventTimeColumns(dfType)
    val tranforms = Map("event_time" -> col(eventTime), "event_type" -> lit(dfType))

    DataFrames.addTransformations(df, tranforms)
  }

  /**
   *
   * @param df The union dataframe containg the deployments, pickups, rides events (with the addConfCols)
   * @return returns a Dataframe with the deployment_id, pickup_id and qr_code set up for all events
   *         The rules for each one are:
   *         deployment_id and time_cycle_started : gets the last (non nulls) ocurred before the current event(current event included)
   *         pickup_id, qr_code and time_cycle_ended: get the first (non nulls) ocurred after the current event(current event included)
   *         since deployment_id just occurrs on deployment events, and pickup_id and qr_code just for pickup_events
   *         the logic will set up the correct ids for all events
   *
   */

  def buidCycle(df: DataFrame): DataFrame = {

    val window = Window.partitionBy("vehicle_id").orderBy("event_time")
    val preceding = window.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    val following = window.rowsBetween(Window.currentRow, Window.unboundedFollowing)
    df
      .withColumn("deployment_id_1", last(col("deployment_id"), true) over preceding)
      .withColumn("time_cycle_started_1", last(col("time_cycle_started"), true) over preceding)
      .withColumn("pickup_id_1", first(col("pickup_id"), true) over following)
      .withColumn("qr_code_1", first("qr_code", true) over following)
      .withColumn("time_cycle_ended_1", first(col("time_cycle_ended"), true) over following)
      .withColumn("deployment_id", col("deployment_id_1"))
      .withColumn("previous_event_cycle_type", last(
                                                      when(col("event_type").isin("deployments", "pickups") , col("event_type"))
                                                      ,true) over preceding)
      .withColumn("pickup_id", col("pickup_id_1"))
      .withColumn("qr_code", col("qr_code_1"))
      .withColumn("time_cycle_ended", col("time_cycle_ended_1"))
      .withColumn("time_cycle_started", col("time_cycle_started_1"))
  }

  /**
   *
   * @param df the dataframe after setColumns transformation
   * @return a new dataframe with the anomalies excluded. Anomalies can be:
   *         Rows with no deployment associated
   *         Rows that have different pickups than the first pickup after the deployment
   *         Rows that have different deployments that the last deployment before the pickup
   *         Rides that happened after a cycle ends
   */
  def cleanAnomalies(df: DataFrame): DataFrame = {
    val windowDeployment = Window.partitionBy("vehicle_id", "deployment_id").orderBy("event_time")
    val windowPickup = Window.partitionBy("vehicle_id", "pickup_id").orderBy("event_time")
    val deploymentUnbounded = windowDeployment.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    val pickupUnbounded = windowPickup.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    //filter out rows without deploy
    val withDepolyment = df.filter(col("deployment_id").isNotNull)

    /**
     * filter out the rows where the minimal time_cycle_ended (by deployment)  is diff from the actual time_cycle_ended
     * those rows represent two or more consecutive pickups wih no deployment between then.
     * So only the rows with the first pickup are considered.
     */
    val pickupsUnique = withDepolyment
      .withColumn("min_time_cycle_ended", min("time_cycle_ended") over deploymentUnbounded)
      .filter(col("time_cycle_ended").isNull || col("time_cycle_ended") === col("min_time_cycle_ended"))
      .drop("min_time_cycle_ended")
    /**
     * filter out rows where the maximum time_cycle_started is diff from the actual time_cycle_started
     * those rows represent two or more consecutive deploys with no pickup between then
     * So only the rows with the latest deployment are considered
     */
    val deploymentsUnique = pickupsUnique
      .withColumn("max_time_cycle_started", max("time_cycle_started") over pickupUnbounded)
      .filter(col("time_cycle_started") === col("max_time_cycle_started"))
      .drop("max_time_cycle_started")
    /**
     * filter out all rides that happeneds after cycle end
     */
    val ridesAnonmalies = deploymentsUnique.filter(
      (col("event_type") === "rides" && col("previous_event_cycle_type") =!= "deployments").unary_!
    )

    ridesAnonmalies

  }

  /**
   *
   * @param df the DataFrame with the ids already set by setIds
   * @return return metrics aggregated by the following key: deployment_id, qr_code, vehicle_id
   */

  def aggregateMetrics(df: DataFrame): DataFrame = {
    df.groupBy("vehicle_id", "deployment_id", "qr_code")
      .agg(
        max("time_deployment_created").as("time_deployment_created")
        , max("time_cycle_started").as("time_cycle_started")
        , min("time_cycle_ended").as("time_cycle_ended")
        , min("time_pickup_done").as("time_pickup_done")
        , count("ride_id").as("total_rides")
        , sum("gross_amount").as("total_amount")
        , sum("distance").as("total_distance")
      )
  }

  /**
   *
   * @param dfDeployment deployments df with the struct defined in Extract
   * @param dfPickups pickups df with the struct defined in Extract
   * @param dfRides rides df with the struct defined in Extract
   * @return Apply all transfomations defined, step by step. The transfomations are:
   *         rename the Extract columns to a more business meaningful name
   *         Calculate rides metrics and transformations, see: RidesTransformations
   *         Add event type and event time to the dataframes
   *         perform a union between the dataFrames
   *         build the cycle from the union DF see: SetColumns
   *         filter the anomalies see: cleanAnomalies
   *         aggregate metrics at the cycle level
   *         apply calculations at metrics on cycle level
   *         get the rides rows from the built cycle
   *         Return the cycle DataFrame and the rides DataFrame
   *
   */

  def transformDFs(dfDeployment: DataFrame, dfPickups: DataFrame, dfRides: DataFrame): (DataFrame, DataFrame) = {
    val deploymentsRen = DataFrames.renameCols(dfDeployment, renameDeploymentsCols)
    val pickupsRen = DataFrames.renameCols(dfPickups, renamePickupsCols)
    val ridesRen = DataFrames.renameCols(dfRides, renameRidesCols)

    val ridesCalc = DataFrames.addTransformations(ridesRen, RidesTransformations.columnTransformations)

    val depColAdded = addConfCols(deploymentsRen, "deployments")
    val picColAdded = addConfCols(pickupsRen, "pickups")
    val ridColAdded = addConfCols(ridesCalc, "rides")

    val union = DataFrames.mergeDataFrames(depColAdded :: picColAdded :: ridColAdded :: Nil)


    val cycleBuild = buidCycle(union)
    val noAnomalies = cleanAnomalies(cycleBuild)

    val cycletMetrcis = aggregateMetrics(noAnomalies)

    val cycletMetrcisCalc = DataFrames.addTransformations(cycletMetrcis,CycleTransformations.columnTransformations)

    val ridesOutput =  noAnomalies.filter(col("event_type") === "rides")
      .select(RidesTransformations.rideOutputColumns.head, RidesTransformations.rideOutputColumns.tail:_*)

    (cycletMetrcisCalc,ridesOutput)
  }


}

object RidesTransformations {
  val columnTransformations = Map(
    "distance" -> GeoTransformations.distance(
      col("start_latitude")
      , col("start_longitude")
      , col("end_latitude")
      , col("end_longitude")
    )
    , ("ride_duration" -> TimestampTransformations.diffInSeconds(
      col("time_ride_started")
      , col("time_ride_ended")))
  )
  val rideOutputColumns = Seq(
    "ride_id"
  ,"vehicle_id"
  ,"qr_code"
  ,"deployment_id"
  ,"pickup_id"
  ,"time_ride_started"
  ,"time_ride_ended"
  ,"start_latitude"
  ,"end_latitude"
  ,"start_longitude"
  ,"end_longitude"
  ,"gross_amount"
  ,"distance")
}

object CycleTransformations {
  val columnTransformations = Map(
    "cycle_duration" -> TimestampTransformations.diffInSeconds(
      col("time_cycle_started")
    , col("time_cycle_ended")
  )
    , "deployment_duration" -> TimestampTransformations.diffInSeconds(
      col("time_deployment_created")
      , col("time_cycle_started")
    )
    , "pickup_duration" -> TimestampTransformations.diffInSeconds(
      col("time_cycle_ended")
      , col("time_pickup_done")
    )
  )
}


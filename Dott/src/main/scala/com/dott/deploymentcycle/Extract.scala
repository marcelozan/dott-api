package com.dott.deploymentcycle

import com.dott.utils.spark.sql.DataFrames
import com.dott.utils.storage.Read
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.functions._

object Extract extends Read with ApplicationParameters  {


  //raw schemas
   val deploymentsSchema = StructType(
    Seq(StructField("task_id", StringType, true),
      StructField("vehicle_id", StringType, true),
      StructField("time_task_created", TimestampType, true),
      StructField("time_task_resolved", TimestampType, true)
    )
  )
   val pickupsSchema = StructType(
    StructField("task_id", StringType, true) ::
      StructField("vehicle_id", StringType, true) ::
      StructField("qr_code", StringType, true) ::
      StructField("time_task_created", TimestampType, true) ::
      StructField("time_task_resolved", TimestampType, true) :: Nil
  )


   val ridesSchema = StructType(
    StructField("ride_id", StringType, true) ::
      StructField("vehicle_id", StringType, true) ::
      StructField("time_ride_start", TimestampType, true) ::
      StructField("time_ride_end", TimestampType, true) ::
      StructField("start_lat", DoubleType, true) ::
      StructField("start_lng", DoubleType, true) ::
      StructField("end_lat", DoubleType, true) ::
      StructField("end_lng", DoubleType, true) ::
      StructField("gross_amount", DoubleType, true) :: Nil
  )

  /**
   *
   * @param col Timestamp column to remove ''UTC''
   * @return a column with no spaces to the left or to the right, and the ''UTC'' removed
   */
 def cleanTimestamp(col: Column) : Column = {
  trim(regexp_replace(col, "UTC", ""))
 }

  /**
   *
   * @param file name of the file to be loaded
   * @param schema the schema of the returning DataFrame
   * @return Dataframe with the schema enforced, for Timestamp columns remove the ''UTC'' value
   */

  def read(file: String, schema: StructType): DataFrame = {
    val raw = readCSV(sourceFolder + file)
    val fieldToCol = schema.map(f => if (f.dataType == TimestampType) cleanTimestamp(col(f.name)).as(f.name) else col(f.name))
    DataFrames.castSchema(raw.select(fieldToCol:_*), schema)
  }


}

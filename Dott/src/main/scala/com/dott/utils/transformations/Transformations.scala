package com.dott.utils.transformations

import org.apache.spark.sql.Column
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.functions._

object TimestampTransformations {
  /**
   *
   * @param start the timestamp that will be subtracted
   * @param end the timestamp that will be subtract from
   * @return returns the diference in seconds between the end and start
   */
  def diffInSeconds(start: Column, end: Column): Column = end.cast(LongType) - start.cast(LongType)
}

object GeoTransformations {
  private val radiusOfEarth = 6371000
  def distance(
              startLat: Column
              ,starLon: Column
              ,endLat: Column
              ,endLon: Column
              ): Column =
  {
    val deltaLat = radians(endLat - startLat)
    val deltaLon = radians(endLon - starLon)
    val dLatSin= sin(deltaLat / 2)
    val dLonSin = sin(deltaLon / 2)
    val a = dLatSin * dLatSin + cos(radians(startLat)) * cos(radians(endLat)) * dLonSin * dLonSin
    val c = atan2(sqrt(a), sqrt(lit(1) - a)) * 2
    c * radiusOfEarth
  }
}

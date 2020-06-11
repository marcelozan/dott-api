package com.dott.utils.dq

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

/**
 *
 * @param name name/reason description of the quality check
 * @param condition column/expression that perform the quality check
 *                  if the condition is true, the row will not be flagged
 *                  if the condition is false, an entry will be created in the
 *                  quality check array
 */
case class Rule(name: String, condition: Column)

/**
 * Rules has some generic quality checks
 */
object Rules {
  /**
   *
   * @param key the key that will be used to check duplicity
   * @param order the column to order the rows by key
   * @return will return true for the most recent row by key, and false for others
   */
  def duplicate(key: Column, order: Column): Column = {
    val window = Window.partitionBy(key).orderBy(order.desc)
    val rn = row_number() over window

    rn === 1
  }

  /**
   *
   * @param col column to be tested
   * @return will return true if the column is not null
   */

  def notNullCol(col: Column) : Column = col.isNotNull
}

/**
 * GeoCoordRules has some rules for geolocation coordinates
 */
object GeoCoordRules {
  private val maxLat = 90.0
  private val maxLon = 180.0

  /**
   *
   * @param lat the column latitude to be tested
   * @return will return true if the value is between -90 and 90
   */

  def validLat(lat: Column) : Column = lat.between(-maxLat, maxLat)

  /**
   *
   * @param lon the column longitude to be tested
   * @return will return true if the value is between -180 and 180
   */
  def validLon(lon: Column) : Column = lon.between(-maxLon, maxLon)
}

/**
 * TimeRules has some timestamps validation
 */
object TimeRules {
  /**
   *
   * @param start the start time of an event
   * @param end the end time of an event
   * @return will return true if the start is less than or equal end time
   */
  def startEndTimes(start: Column, end: Column): Column = start <= end
}

/**
 * Builder will create a array column with the failed conditions
 */
object Builder {
  /**
   *
   * @param rule the rule performed on a column
   * @return if the test passed (true) returns null value, otherwise returns the name of the rule
   */
  private def reason(rule: Rule): Column = when(rule.condition, null).otherwise(rule.name)

  /**
   *
   * @param df the dataframe that will be checked
   * @param rules a sequence of rules to be performed on the dataframe
   * @param checksCol the name of the column for output the tests results
   * @return return a dataframe with a array column added. This new column has the test name failed.
   */

  def arrayBuild(df: DataFrame, rules: Seq[Rule], checksCol: String) : DataFrame = {
    val reasons = rules.map(reason)
    df.withColumn(checksCol, array_except(array(reasons:_*), array(lit(null))))
  }
}



package com.dott.utils.storage

import org.apache.spark.sql.{DataFrame, SparkSession}

class Write {
  /**
   * gets the active sparkSession
   */
  private val spark = SparkSession.builder().getOrCreate()

  def writeCSV(df: DataFrame, path: String) = {
    df.coalesce(1) // the coalesce is just for the assigment, on that way Spark will generate just one file
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(path)
  }

  def writeJson(df: DataFrame, path: String) = {
    df.coalesce(1) // the coalesce is just for the assigment, on that way Spark will generate just one file
      .write
      .mode("overwrite")
      .json(path)
  }
}



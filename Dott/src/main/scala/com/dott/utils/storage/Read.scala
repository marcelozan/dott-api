package com.dott.utils.storage

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

class Read {
  /**
   * gets the active sparkSession
   */
  private val spark = SparkSession.builder().getOrCreate()

  /**
   *
   * @param path the absolute path for the csv files
   * @param header if the csv file contains a header or not, default true
   * @param delimiter the column delimeter for the csv file, defalut `,`
   * @return returns a Dataframe with the columns and rows of the files contained on path parameter
   */
  def readCSV(path: String,
              header: Boolean = true,
              delimiter: String = ",") : DataFrame = {
    spark.read
      .format("csv")
      .option("header", header)
      .option("delimiter", delimiter)
      .load(path)
  }

}

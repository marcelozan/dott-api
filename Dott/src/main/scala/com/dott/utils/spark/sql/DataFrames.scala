package com.dott.utils.spark.sql

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

object DataFrames {

  /**
   *
   * @param df Datafrmame with the columns to be rename
   * @param colsRename Map with the key as the daframe's column, and value column's name to be
   * @return the Dataframe with the columns renamed
   */

  def renameCols(df: DataFrame, colsRename: Map[String, String] = Map()): DataFrame = {
    if (colsRename.isEmpty) df
    else {
      val colNames = df.columns
      val cols = colNames.map(name => col(name).as(colsRename.getOrElse(name, name)))
      df.select(cols:_*)
    }
  }

  /**
   *
   * @param dfs Seq of dataframes with the same schema
   * @return the single dataframe from the union all of all dataframes in parameter
   */

  def unionDataFrames(dfs: Seq[DataFrame]): DataFrame = dfs.reduceLeft(_.union(_))

  /**
   *
   * @param df Dataframe with the original schema, to be conformed with another schema
   * @param cols columns from conformed schema (all columns from 2 dataframes i.e)
   * @return a new dataframe with the all the columns from cols. When the original
   *         dataframe does not have the col, map it to null
   */
  def conformDataFrame(df: DataFrame, cols: Seq[String]): DataFrame = {
    val inputCols = df.columns
    val confCols = cols.map(c => c match {
      case name if inputCols.contains(name) => col(name).as(name)
      case _ => lit(null).as(c)
    })
    df.select(confCols:_*)
  }

  /**
   *
   * @param dfs Seq of datframes with diferente schemas
   * @return a single dataframe resulting from the union of the input dataframes
   *         IMPORTANT this function do not convert different datatypes, it was assumed
   *         the columns with the same name, has the same datatype
   */
  def mergeDataFrames(dfs: Seq[DataFrame]): DataFrame = {
    val allCols = dfs.flatMap(_.columns).distinct
    val confDFs = dfs.map(df => conformDataFrame(df, allCols))
    unionDataFrames(confDFs)
  }

  /**
   *
   * @param df the original dataframe
   * @param transformations a Map from the name of the new column in the dataframe, to the col expression to calculate it
   * @return a new dataframe with all the columns added to the original dataframe
   *
   */

  def addTransformations(df: DataFrame, transformations: Map[String, Column]) : DataFrame = {
    transformations.foldLeft(df)((acc, t) =>
    acc.withColumn(t._1, t._2))
  }

  def castSchema(df: DataFrame, schema: StructType) : DataFrame = {
    val cols = schema.map(f => col(f.name).cast(f.dataType).as(f.name))
    df.select(cols:_*)
  }



}

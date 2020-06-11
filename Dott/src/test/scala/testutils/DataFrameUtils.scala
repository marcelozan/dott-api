package testutils

import com.holdenkarau.spark.testing.Column
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.StructType

trait DataFrameUtils {
  def create(input: Seq[Row], schema: StructType, spark: SparkSession) = {
    val rdd = spark.sparkContext.parallelize(input)
    val df = spark.createDataFrame(rdd, schema)
    df
  }




}

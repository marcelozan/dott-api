package testutils

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

/**
 * Spark fixture, creates a session before running a suite and cleans up after it
 *
 * Contains spark related definitions such as SparkOperations, SparkSession and the respective SparkConf
 */
trait SparkSessionProvider extends BeforeAndAfterAll {
  this: Suite =>

  def appID: String = (this.getClass.getName
    + math.floor(math.random * 10E4).toLong.toString)


def logLevel: String = "WARN"

  def sparkConf: SparkConf
/**
  var sparkSession: SparkSession = _

  override def beforeAll(): Unit = {
    sparkSession = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel(logLevel)

    super.beforeAll()
  }

  override def afterAll(): Unit = {
    try super.afterAll()
    finally {
      if (sparkSession != null) {
        sparkSession.stop()
        sparkSession = null
      }
    }
  }

 **/
}
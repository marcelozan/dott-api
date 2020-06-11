import com.dott.deploymentcycle.{DataClean, Extract, Load, Transformations}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

object Main {




  def main(args: Array[String]): Unit = {

    /**
     * The first thing is to create a SparkSession that will be reused in the entire application
     * app name, master and partition number, should be defined at the spark-submit call on a production environment
     * for the sake of the assignment, they were set up here.
     */
    val spark = SparkSession.builder()
      .appName("DottDeploymentCycle")
      .config("spark.master", "local")
      .config("spark.sql.shuffle.partitions", "5")
      .getOrCreate()
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)


    //extract from files
    val rawDeployments = Extract.read(Extract.deploymentsFileName, Extract.deploymentsSchema)
    val rawPickups = Extract.read(Extract.pickupsFileName, Extract.pickupsSchema)
    val rawRides = Extract.read(Extract.ridesFileName, Extract.ridesSchema)


    //dataquality checks

    val cleanedDeployments = DataClean.dataQualityDeployments(rawDeployments)
    val cleanedPickups = DataClean.dataQualityPickups(rawPickups)
    val cleanedRides = DataClean.dataQualityRides(rawRides)

    //transformations
    val (cycles, rides) = Transformations.transformDFs(cleanedDeployments, cleanedPickups, cleanedRides)

    Load.loadCycles(cycles)
    Load.loadRides(rides)







    spark.stop()




  }

}

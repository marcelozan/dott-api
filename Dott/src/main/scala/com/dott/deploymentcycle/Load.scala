package com.dott.deploymentcycle

import com.dott.utils.storage.Write
import org.apache.spark.sql.DataFrame

object Load extends Write with ApplicationParameters {
  /**
   *
   * @param df Dataframe with the cycles to be loaded
   */
  def loadCycles(df: DataFrame) = {
    writeCSV(df, cycleFolder)
  }

  /**
   *
   * @param df dataFrame with the rides to be loaded
   */

  def loadRides(df: DataFrame) = {
    writeCSV(df, rideFolder)
  }

}

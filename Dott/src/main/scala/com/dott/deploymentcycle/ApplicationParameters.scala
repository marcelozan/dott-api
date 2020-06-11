package com.dott.deploymentcycle

trait ApplicationParameters {

  //source folder
  val sourceFolder = "../data-engineer-test/"
  //source file names
  val deploymentsFileName = "deployments*.csv"
  val pickupsFileName = "pickups*.csv"
  val ridesFileName = "rides*.csv"

  //target folders
  val cycleFolder = "../target/cycle/"
  val rideFolder = "../target/ride/"

  //reject folder
  val rejectDeploymentsFolder = "../reject/deployments/"
  val rejectPickupsFolder = "../reject/pickups/"
  val rejectRidesFolder = "../reject/rides/"


}

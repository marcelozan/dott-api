name := "Dott"

version := "0.1"

scalaVersion := "2.11.12"

//Spark dependencies
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.4",
  "org.apache.spark" %% "spark-sql" % "2.4.4"
)


// TESTS
libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  "org.mockito" %% "mockito-scala-scalatest" % "1.5.2" % Test,
  "com.holdenkarau" %% "spark-testing-base" % "2.4.3_0.12.0" % Test
)
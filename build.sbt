name := "mfour-big-data-candidate-test"

version := "0.1"

scalaVersion := "2.11.12"

val spark = "2.3.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % spark,
  "org.apache.spark" %% "spark-sql" % spark,
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  "org.apache.logging.log4j" % "log4j-api" % "2.11.1",
  "org.apache.logging.log4j" % "log4j-core" % "2.11.1"
)

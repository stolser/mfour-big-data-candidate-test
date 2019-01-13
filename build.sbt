
name := "mfour-big-data-candidate-test"

version := "0.1"

scalaVersion := "2.11.12"

val spark = "2.3.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % spark,
  "org.apache.spark" %% "spark-sql" % spark,
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  "com.typesafe" % "config" % "1.3.1"
)

// sbt-assembly plugin settings
assemblyOption in assembly := (assemblyOption in assembly).value.copy(cacheUnzip = false)

assemblyExcludedJars in assembly := {
  val cp = (fullClasspath in assembly).value
  cp filter {f => f.data.getName.startsWith("scala-")}
}

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

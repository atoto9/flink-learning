name := "flink-learning"

version := "0.1"

scalaVersion := "2.12.8"

val flinkVersion = "1.9.1"

libraryDependencies ++= Seq(
  "org.jsoup" % "jsoup" % "1.8.3",
  "org.scalactic" %% "scalactic" % "3.0.8",
  "org.scalatest" %% "scalatest" % "3.0.8" % "test",
  "org.slf4j" % "slf4j-log4j12" % "1.2" % Test,
  "org.apache.flink" %% "flink-scala" % flinkVersion,
  "org.apache.flink" %% "flink-clients" % flinkVersion,
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion,
  "org.apache.flink" %% "flink-connector-kafka" % flinkVersion,
  "org.apache.flink" %% "flink-statebackend-rocksdb" % flinkVersion,
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.1.4",
  "org.apache.flink" %% "flink-cep-scala" % flinkVersion,

)

assemblyMergeStrategy in assembly := {
  case x if x.contains("versions.properties") => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

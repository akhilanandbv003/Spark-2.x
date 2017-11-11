name := "SparkTw0"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.2.0"

val kafkaSparkVersion = "0-10_2.11"


/*
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % sparkVersion
//"org.apache.kafka" % "kafka-clients" % "0.11.0.1"
)*/
libraryDependencies ++= Seq("org.apache.spark" % "spark-sql_2.11" % "2.2.0",
  //"org.apache.spark" % "spark-core_2.11" % "2.2.0",

"org.apache.spark" % "spark-sql-kafka-0-10_2.11" % "2.2.0")
 // "org.apache.kafka" % "kafka-clients" % "0.11.0.1")

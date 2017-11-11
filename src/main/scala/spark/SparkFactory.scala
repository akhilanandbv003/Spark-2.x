package spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

object SparkFactory {
  def getAndConfigureSparkSession() = {
    val conf = new SparkConf()
      .setAppName("Structured Streaming")//My App name
      .setMaster("local[*]")
      //.set("spark.cassandra.connection.host", "127.0.0.1")
      .set("spark.sql.streaming.checkpointLocation", "checkpoint")

    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    SparkSession
      .builder()
      .getOrCreate()

  }

  def getSparkSession() = {
    SparkSession
      .builder()
      .getOrCreate()
  }
  implicit val schema = StructType(Array(
    StructField("id", StringType),
    StructField("name", StringType)
  ))
}

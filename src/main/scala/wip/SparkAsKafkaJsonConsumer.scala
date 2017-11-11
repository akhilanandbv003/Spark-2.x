package wip

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode

object SparkAsKafkaJsonConsumer {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder
      .master("local")
      .appName("SparkAsKafkaJsonConsumer")
      .getOrCreate()
    import sparkSession.implicits._
    val schema = StructType(Array(
      //Date,Open,High,Low,Close,Volume,Adj
      StructField("Date", StringType),
      StructField("Open", StringType),
      StructField("High", StringType),
      StructField("Low", StringType),
      StructField("Close", StringType),
      StructField("Volume", StringType),
      StructField("AdjClose", StringType)
    ))


    val df = sparkSession
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "CSVProducer")
      .option("startingOffsets", "earliest")
      .load()

    df.printSchema()
    /*val df1 = df.selectExpr("CAST(value AS STRING) as json")
                .select(from_json('json,schema).as("data"))
                .selectExpr("data.*")
*/
    val df1 = df.selectExpr("CAST(value AS STRING)").as[String]




    val query = df1.writeStream
      .format("console")
      .option("truncate", "false")
      .outputMode(OutputMode.Append())
      .start()

    query.awaitTermination()

  }

}

package sparkApps

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/*
Run Docker
Create Kafka Console Producer:

  cd /Users/avenk3/Downloads/confluent-3.3.0/bin

Create a topic
  ./kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test

Create a console producer
./kafka-console-producer --broker-list localhost:9092 --topic test

Read from a file
./kafka-console-producer --broker-list:9092 --topic customer < /Users/avenk3/spark/My-Projects/spark2.2/src/main/resources/customers.csv
*/

object SparkConsumer {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder
      .master("local")
      .appName("Consumer")
      .getOrCreate()

    val schema = StructType(Array(
      StructField("id", StringType),
      StructField("name", StringType)
    ))
    import sparkSession.implicits._

    val df = sparkSession
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "customer")
      .option("startingOffsets", "earliest")
      .load()

    val df1 = df.selectExpr("CAST(value AS STRING)").as[String]


    val query = df1.writeStream
      .format("console")
      .option("truncate", "false")
        .outputMode(OutputMode.Append())
      .start()

    query.awaitTermination()

  }

}
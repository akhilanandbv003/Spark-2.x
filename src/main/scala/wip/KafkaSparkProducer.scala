package wip

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{StringType, StructField, StructType, _}
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession


//  ~/Downloads/confluent-3.3.0/bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic topic1 --from-beginning
//./kafka-topics --list --zookeeper localhost:2181
//./kafka-topics --delete --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test

object KafkaSparkProducer {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder
      .master("local")
      .appName("Consumer")
      .getOrCreate()
    import sparkSession.implicits._

    val schema = StructType(
      StructField("id", StringType) ::
        StructField("name", StringType) :: Nil)


    //case class Apple(Date: String, Open: String, High: String, Low: String, Close: String, Volume: String, AdjClose: String)



    //create stream from folder
    val fileStreamDf = sparkSession.readStream
      .option("header", "true")
      .schema(schema)
      .csv("/Users/avenk3/spark/My-Projects/spark2.2/src/main/resources/csv")

    /* //Console Sink
    val query = ds.writeStream
      .format("console")
      .outputMode(OutputMode.Append()).start()
    query.awaitTermination()
   */
//val ds=fileStreamDf.selectExpr("CAST(id AS STRING)", "CAST(name AS STRING) as value")


    //Kafka sink
    val q= fileStreamDf.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "akhil")
     .option("checkpointLocation","/tmp/")
      .start()

    q.awaitTermination()
  }

}
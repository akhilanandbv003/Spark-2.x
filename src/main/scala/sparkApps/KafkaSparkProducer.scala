package sparkApps

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType


//  ~/Downloads/confluent-3.3.0/bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic topic1 --from-beginning
//./kafka-topics --list --zookeeper localhost:2181
//./kafka-topics --delete --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test

object KafkaSparkProducer {

  case class Person(id: String, name: String)

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder
      .master("local")
      .appName("Consumer")
      .getOrCreate()
    import sparkSession.implicits._


    val caseSchema = ScalaReflection
      .schemaFor[Person]
      .dataType
      .asInstanceOf[StructType]


    //create stream from folder
    val fileStreamDf = sparkSession.readStream
      .schema(caseSchema)
      .option("header", "true")
      .csv("/Users/avenk3/mygithub/Spark-2.x/src/main/resources/csv")
      .as[Person]

    /* //Console Sink
    val query = fileStreamDf.writeStream
      .format("console")
      .outputMode(OutputMode.Append()).start

    query.awaitTermination()*/


    val ds = fileStreamDf.selectExpr("CAST(id AS STRING)", "CAST(name AS STRING) as value")

    //Kafka sink
    val q = ds.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "akhil")
      .option("checkpointLocation", "/tmp/")
      .start()

    q.awaitTermination()
  }

}
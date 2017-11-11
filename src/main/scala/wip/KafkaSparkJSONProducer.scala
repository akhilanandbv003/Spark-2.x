package wip

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

import org.apache.spark.SparkException
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, SpecificInternalRow, UnsafeProjection}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types.{BinaryType, DataType}

//  ~/Downloads/confluent-3.3.0/bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic topic1 --from-beginning
//./kafka-topics --list --zookeeper localhost:2181
//./kafka-topics --delete --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test

object KafkaSparkJSONProducer {

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


    //val ds = fileStreamDf.selectExpr("CAST(id AS STRING)", "CAST(name AS STRING) as value")
    val ds= fileStreamDf.withColumn("value", struct(fileStreamDf("a"), fileStreamDf("b")))

    //Kafka sink
    val q = ds.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "amar")
      .option("checkpointLocation", "/tmp/")
      .start()

    q.awaitTermination()
  }

}
package stackoverflow

import java.sql.Timestamp

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{StringType, StructField, StructType}


object windowOperation {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder
      .master("local")
      .appName("stackoverflow")
      .getOrCreate()
    import sparkSession.implicits._
    val schema = StructType(
      Array
      (
        StructField("state", StringType),
        StructField("tweet", StringType)
      )
    )

    val tweet= col("tweet")
    val state= col("state")

    val tweetsDF = sparkSession.read
      .format("json")
      .option("mode", "FAILFAST")
      .schema(schema)
        .option("multiLine","true")
      .load("/Users/avenk3/mygithub/Spark-2.x/src/main/resources/json/tweets.json")


     val socketDs: Dataset[Tweets] = tweetsDF.as[Tweets]
    //socketDs.printSchema()

    val wordsDs = socketDs

    wordsDs.printSchema()

    val windowedCount= wordsDs.groupBy('state)//.agg(sum('tweets))
windowedCount

   /* val query =
      tweetsDF.writeStream
        .format("console")
        .option("truncate","false")
        .outputMode(OutputMode.Complete())

query.start().awaitTermination()*/
}
}

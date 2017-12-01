package datasets

import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode

/**
  * Created by avenk3 on 11/19/17.
  */
object jsonDs {
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

    val tweetsDF: DataFrame = sparkSession.readStream
      .format("json")
      .option("mode", "FAILFAST")
      .schema(schema)
      .option("multiLine","true")
      .load("/Users/avenk3/mygithub/Spark-2.x/src/main/resources/json/")

   // val tweetsDs =tweetsDF.as[Tweets]
   val currentTimeDf = tweetsDF.withColumn("processingTime", current_timestamp())



    val wc = currentTimeDf.groupBy(
      window($"processingTime", "1 minutes", "1 minutes"),
      $"state",
    $"tweet"
    )
      .count()
      .orderBy("window")
      .orderBy($"state")
      .orderBy($"tweet")



    val query =
      wc.writeStream
        .format("console")
        .option("truncate", "false")
    .outputMode(OutputMode.Complete())


    query.start().awaitTermination()









































    /* val sc = tweetsDs.map(x => {
      val state: String = x.state
      val tweets = x.tweet
      //println(state)
      //println(tweets)
      val count = tweets.split(" ").length
      (state,count)
    })

    val re: RelationalGroupedDataset = sc.groupBy($"_1")
    println(re)

      */
  }
}

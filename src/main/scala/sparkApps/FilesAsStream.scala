package sparkApps

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{StringType, StructField, StructType}


object FilesAsStream {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder
      .master("local")
      .appName("example")
      .getOrCreate()

    val schema = StructType(
      Array(StructField("transactionId", StringType),
        StructField("customerId", StringType),
        StructField("itemId", StringType),
        StructField("amountPaid", StringType)))

    //create stream from folder
    val fileStreamDf = sparkSession.readStream
      .option("header", "true")
      .schema(schema)
      .csv("/Users/avenk3/spark/CCA175/spark2/src/main/resources/csv")
    //Place the CSV in this folder and stream doesn't detect changes in csv.
    //Only detects new csv files with same schema

    val query = fileStreamDf.writeStream
      .format("console")
      .outputMode(OutputMode.Append()).start()

    query.awaitTermination()

  }

}
package source

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.OutputMode
import schemas.Schema
import spark.SparkFactory

/**
  * Read CSV files and store it as Parquet
  * Simulate a parquet stream as source
  */
object DataIngest {

  /*def main(args: Array[String]) {
    	    val spark = SparkFactory.getAndConfigureSparkSession()
    readParquet
    spark.streams.awaitAnyTermination()
  }*/


  val sfoFireDeptCalls = "/Users/avenk3/mygithub/Spark-2.x/src/main/resources/csv"
  val sfoFireDeptCallsSchema = Schema.getSchema()

  def readCsv(inputSource: String): DataFrame = {
    val spark = SparkFactory.getSparkSession()

    val fileStreamDf = spark.readStream
      .option("header", "true")
      .schema(sfoFireDeptCallsSchema)
      .csv(inputSource)

    fileStreamDf.printSchema()
    fileStreamDf
  }

  def writeParquet(csvDataFrame: DataFrame) = {

    val write = csvDataFrame.writeStream
      .format("parquet") // can be "orc", "json", "csv", etc.
      .option("path", "/Users/avenk3/mygithub/Spark-2.x/src/main/resources/sfoFireDeptCalls.parquet")
      .start()
    write.awaitTermination()
  }


  def debugReadCSV(readCsv: DataFrame) = {
    val query = readCsv.writeStream
      .format("console")
      .outputMode(OutputMode.Append()).start()
    query.awaitTermination()
  }

  //readCsv(sfoFireDeptCalls)




}

import sink.CassandraSink
import source.DataIngest
import spark.{Analytics, SparkFactory}
import sparkApps.SparkConsumer

object Main {

/*
* Read SFO Fire Department Calls for Service Dataset
* 4.49M Rows and has 34 Columns
*
* Read CSV files and store it as Parquet
* Simulate a parquet stream as source
* Perform analytics on the stream
* Write it to Cassandra
*
* Future work:
* Write the parquet stream data into a  kafka topic
* Read the kafka topic
*
* Analytics:
* Q-1) How many different types of calls were made to the Fire Department?
* Q-2) How many incidents of each call type were there?
* Q-3) How many years of Fire Service Calls is in the data file?
* Q-4) How many service calls were logged in the past 7 days?
* Q-5) Which neighborhood in SF generated the most calls last year?
* Q-6) What was the primary non-medical reason most people called the fire department from the Tenderloin last year?
* Q-7) What do residents of Russian Hill call the fire department for?
* */
  def main(args: Array[String]): Unit = {
    val spark = SparkFactory.getAndConfigureSparkSession()

    //Generate a stream from a parquet file
    val inputDF = Analytics.readParquet()
    inputDF.show(20,false)
    inputDF.printSchema()


   /* //Read the input DF and perform analytics
    val analyticsResult = Analytics.returnResult()

    //Write the result to Cassandra
    CassandraSink.writeResults
*/

  }
}

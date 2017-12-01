package sparkApps

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{StringType, StructField, StructType}
//import org.apache.spark.sql.hive.HiveContext
//import com.holdenkarau.spark.testing.DataFrameSuiteBase
//import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
//import org.scalatest.FunSuite


object JsonRead{ //extends FunSuite with DataFrameSuiteBase{
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder
      .master("local")
      .appName("example")
      .getOrCreate()

    //create stream from folder
    val jsonDf = sparkSession
      .read
      .option("wholeFile", true)
      .option("mode", "FAILFAST")
        .option("allowComments",true)
        .option("allowBackslashEscapingAnyCharacter",true)
        .option("allowBackslashEscapingAnyCharacter",true)
      .json("/Users/avenk3/mygithub/Spark-2.x/src/main/resources/json/tweets.json")

    jsonDf.printSchema()

    jsonDf.createOrReplaceTempView("jsonTable")
    //HiveContext sqlContext.sql("create table mytable as select * from mytempTable");


  }



}
package spark

import source.DataIngest.{readCsv, sfoFireDeptCalls, writeParquet}

/**
  * Analytics:
  * Q-1) How many different types of calls were made to the Fire Department?
  * Q-2) How many incidents of each call type were there?
  * Q-3) How many years of Fire Service Calls is in the data file?
  * Q-4) How many service calls were logged in the past 7 days?
  * Q-5) Which neighborhood in SF generated the most calls last year?
  * Q-6) What was the primary non-medical reason most people called the fire department from the Tenderloin last year?
  * Q-7) What do residents of Russian Hill call the fire department for?
  */
object Analytics {

  def readParquet() = {
    val spark = SparkFactory.getSparkSession()
    //val csvDataFrame = readCsv(sfoFireDeptCalls)
    //val writeParquetFile = writeParquet(csvDataFrame)
    val parquetFileDF = spark.read.parquet("/Users/avenk3/mygithub/Spark-2.x/src/main/resources/sfoFireDeptCalls.parquet")


    parquetFileDF.createOrReplaceTempView("parquetFile")
    val namesDF = spark.sql("SELECT * FROM parquetFile")
    namesDF

  }

  def returnResult(): Unit ={

  }
}

package datasets

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode

object lastday extends App{
 /*
  I want to retrieve the last 24-hours data from my DataFrame.

However, I do not know how to convert datetime to milliseconds using Spark SQL (Spark 2.2.0 and Scala 2.11).

I can do it using DataFrame, but don't know how to merge everything together:


  val data = spark.read.parquet(path_to_parquet_file)
  data.createOrReplaceTempView("table")

  var df = spark.sql("SELECT datetime, product_PK FROM table WHERE datetime BETWEEN (datetime - 24*3600000) AND datetime")

  import org.apache.spark.sql.functions.unix_timestamp

df = df.withColumn("unix_timestamp",unix_timestamp(col("datetime"))).drop("datetime")







*/
 val sparkSession = SparkSession.builder
   .master("local")
   .appName("stackoverflow")
   .getOrCreate()
  import sparkSession.implicits._
/*
 val fileStreamDf: DataFrame = sparkSession.read
   .option("header", "true")
     .option("inferschema",true)
   .csv("/Users/avenk3/spark/CCA175/spark2/src/main/resources/sales.csv")

*/

  //val df = fileStreamDf.withColumn("unix_timestamp",unix_timestamp(col("datetime"))).drop("datetime")

 val x = sparkSession.sql("SELECT unix_timestamp() as unix_timestamp")
x.show()

 val y = sparkSession.sql("SELECT current_timestamp() as current_timestamp").show(truncate = false)


 sparkSession.sql(s"""SELECT from_unixtime(timestamp, '% Y % D % M % h :% i :% s)""").show(truncate = false)

 /*sparkSession.sql(s"""SELECT DATE_FORMAT($x),
                 'yyyy-MM-dd' )""").show()
*/

/* val a: DataFrame = (Seq(("abc", 123), ("cde", 23))).toDF("column_1", "column_2")
 val aWithId: DataFrame = a.withColumn("id",monotonically_increasing_id())

 val b: DataFrame = (Seq((1), (2))).toDF("column_3")
 val bWithId: DataFrame = b.withColumn("id",monotonically_increasing_id())

 val x1 = aWithId.join(bWithId, "id")


 x1.show()*/

 //fileStreamDf.write.mode(SaveMode.Overwrite).parquet("/Users/avenk3/Desktop/test folder/test.parquet")
//val x ="2807900401380217040100004V00004V00003V00003V00003V00003V00003V00004V00004V00004V00004V00005V00005V00005V00005V00004V00005V00004V00005V00006V00006V00006V00005V00004V 2807900401380217040200004V00004V00004V00004V00003V00003V00004V00005V00006V00007V00004V00004V00005V00005V00005V00005V00004V00004V00004V00005V00005V00005V00005V00004V"

//x.split("2807900")
}

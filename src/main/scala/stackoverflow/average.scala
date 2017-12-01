package stackoverflow

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
/**
  * Created by avenk3 on 11/30/17.
  */

object average extends App{

  val sparkSession = SparkSession.builder
    .master("local")
    .appName("example")
    .getOrCreate()

  import sparkSession.implicits._


      val x = sparkSession.read
        .option("header", "false")
        //.option("inferSchema", "true")
        .option("delimiter", "\\t")
        .option("mode", "FAILFAST")
        //.option("ignoreLeadingWhiteSpace", "true")
        //.option("ignoreTrailingWhiteSpace", "true")
        //.option("nullValue", "true")
        .csv("/Users/avenk3/mygithub/Spark-2.x/src/main/resources/tab_data.csv")




x.printSchema()
x.show(truncate = false)
 val df: DataFrame =  x.select('_c0 as "id",'_c1 as "sub1",'_c2 as "sub2",'_c3 as "sub3",'_c4 as "sub4")

  df.groupBy('id).agg(avg('sub1)).show()
}


/* Write tab limited data

x.write.option("delimiter","\\t")
  .csv("/Users/avenk3/mygithub/Spark-2.x/src/main/resources/data.tsv")
*/

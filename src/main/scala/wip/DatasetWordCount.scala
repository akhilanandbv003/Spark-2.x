package wip

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}


object DatasetWordCount {
  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder.
      master("local")
      .appName("example")
      .getOrCreate()

    val schema = StructType(
      Array(StructField("transactionId", StringType),
        StructField("customerId", StringType),
        StructField("itemId", StringType),
        StructField("amountPaid", StringType)))

//def createSchema()
    val data = sparkSession.read.option("header", "true").
      csv("src/main/resources/sales.csv")


    /* val words = data.flatMap(value => value.split("\\s+"))

     val groupedWords = words.groupByKey(_.toLowerCase)

     val counts = groupedWords.count()

     counts.show()*/

    /*val df = data.selectExpr(
      "transactionId" , "customerId" , "itemId","amountPaid" ,"'temp' as saludo"
    )*/
    var x = "akhil"
    mymeth(data,x).show()

  }

  def mymeth(df: DataFrame , param : String)={
    df.selectExpr(
    "transactionId" , "customerId" , "itemId","amountPaid" ,"'hello' as saludo","'hello' as saludo1")



  }
}

package wip

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}


object saludo {
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

    val data = sparkSession.read.option("header", "true").
      csv("src/main/resources/sales.csv")



    val english = "hello"
    generar_informe(data,english).show()

  }

  def generar_informe(df: DataFrame , english : String)={
    df.selectExpr(
      "transactionId" , "customerId" , "itemId","amountPaid" , s"""'${english}' as saludo """)
  }
}

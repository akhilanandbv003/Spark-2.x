package stackoverflow

import org.apache.commons.cli.OptionBuilder
import org.apache.spark.sql.SparkSession

/**
  * Created by avenk3 on 11/30/17.
  */
object qwndk extends App{
  import org.apache.commons.cli.OptionBuilder

  val sparkSession = SparkSession.builder
    .master("local")
    .appName("example")
    .getOrCreate()



      val x = sparkSession.read.option("header", "true")
        .option("header", "true")
        //.option("inferSchema", "true")
        .csv("/Users/avenk3/mygithub/Spark-2.x/src/main/resources/t_datacent_cus_temp_guizhou_ds_tmp.csv")

  x.printSchema()

  x.createOrReplaceTempView("t_datacent_cus_temp_guizhou_ds_tmp")


      sparkSession.sql(
        s"""
           |  select  cast(tax_file_code as String) as tax_file_code,
           |          cus_name,
           |          cast(tax_identification_number as String) as tax_identification_number
           |  from    t_datacent_cus_temp_guizhou_ds_tmp
  """.stripMargin).createOrReplaceTempView("t_datacent_cus_temp_guizhou_ds")

      sparkSession.sql("select * from t_datacent_cus_temp_guizhou_ds").show()




}

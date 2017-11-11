import spark.SparkFactory
import sparkApps.SparkConsumer

/**
  * Created by avenk3 on 11/9/17.
  */
object Main {


  def main(args: Array[String]): Unit = {
    val spark = SparkFactory.getAndConfigureSparkSession()

    val query= SparkConsumer

  }
}

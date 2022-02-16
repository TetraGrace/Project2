import org.apache.spark.sql.SparkSession

import org.apache.spark.sql._
import scala.io.StdIn._

object Main {
  def main(args: Array[String]):Unit = {

    System.setProperty("hadoop.home.dir", "C:\\winutils")

    val spark = SparkSession
      .builder()
      .appName("Project2")
      .config("spark.master", "local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    println("Hello World")
    //i have made a change to the file, and will push to the grace branch
    //i am not making a change to master
    println("howdy")
    print("Try two")
    println("twy three")

  }
}

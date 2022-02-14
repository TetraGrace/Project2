import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]):Unit = {


    val spark = SparkSession.builder()
      .appName("I wonder if this matters")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.read.json("input/yelp_academic_dataset_business.json")
    df.select("business_id").where("attributes = 'RestaurtantsTakeout'").show(1000)

    println("Hello World")
    //i have made a change to the file, and will push to the grace branch
    //i am not making a change to master
    println("howdy")
    print("Try two")
    println("twy three")
  }
}

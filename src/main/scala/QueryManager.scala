import scala.io.StdIn._
import org.apache.spark.sql._
import com.github.mrpowers.spark.daria.sql.DariaWriters

class QueryManager {

  var yelpBusinessDS = "yelp_businessDS.csv"
  var csvLocation = " "

  def readBusinessData(sparkSession: SparkSession): Unit = {
    val df = sparkSession.read.json("data/yelp_academic_dataset_business.json")
    df.show()
  }

  def exportData(df: DataFrame, sparkSession: SparkSession): Unit = {

    println("Creating Excel file...")
    DariaWriters.writeSingleFile(df = df, format = "csv", sc = sparkSession.sparkContext,
      tmpFolder = MiscManager.outputPath.toString(), filename = MiscManager.outputPath+"\\"+yelpBusinessDS)
    csvLocation = MiscManager.outputPath+"\\"+yelpBusinessDS
  }
}

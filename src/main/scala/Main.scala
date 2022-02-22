
object Main {

  val sparkManager = new SparkManager

  def main(args: Array[String]):Unit = {

    Init()

  }

  def Init():Unit = {
    sparkManager.readBusinessData()
  }
}

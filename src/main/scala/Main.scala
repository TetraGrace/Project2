import Managers.SparkManager
import login.MenuStuff.{Menu, MenuObject}
import login.UsersManagement
import Queries._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

import scala.io.StdIn.readLine
object Main {

  val sparkManager = new SparkManager

  def main(args: Array[String]):Unit = {

       System.setProperty("hadoop.home.dir", "c:/winutils")
       val spark = SparkSession
         .builder()
         .appName("project1")
         .config("spark.master","local")
         .enableHiveSupport()
         .getOrCreate()
       spark.sparkContext.setLogLevel("ERROR")

    val df = spark.read.json("data/yelp_academic_dataset_business.json")

  val a1= MenuObject(1,"t1","City scoring based on businesses in that location, To determine the value of cities based on reviews")
  val a2= MenuObject(2,"t2","Average scoring by cuisine type, To determine the success rate of type of cuisine ")
    val menu= new Menu(List(a1,a2),"Query Menu")
    menu.addMenuOption(MenuObject(3,"t3","Popularity of business based on scores by date range to produces a trendline graph on the popularity of a store based on the check-in"))
    menu.addMenuOption(MenuObject(4,"t4","Rating to review count comparison,To gauge validity of reviews to the avg review score"))
    menu.addMenuOption(MenuObject(5,"t5","Best restaurants & cities by avg review score and review counts, Like a top 10 restaurants listing"))
    menu.addMenuOption(MenuObject(6,"t6","Cuisine to city-level comparison, Best type of cuisine in a city based on ratings"))
    menu.addMenuOption(MenuObject(7,"t7","How a single restaurant (user-input) is positioned compared to other businesses, Ex: Restaurant A is 153rd out of all 19k restaurants based on score and review count"))
    menu.addMenuOption(MenuObject(8,"t8","TO END"))

    //Starting point...
    println("Welcome to the YELP Business Analysis tool..")
    val session=new UsersManagement
    var loop=true

    while(loop) {
      val input1 = readLine("Please choose what you want to do:\n1.Add admin user \n2.Add normal user \n3.skip to Login (admin to do queries, normal or guest can't)\n")
      var permission = ("")
      input1.toInt match {
        case 1 => session.addAdmin
        case 2 => session.addNormal
        case 3 =>
          permission = session.checkPermission(session.prompt)
          println("You are logged in as " + permission + " user!")
          if (permission == "admin") {
            println("You have the permission to see user/password table AND to make queries!")
            session.showTable
            query
            loop=false
          }
          else if (permission=="normal") {
            println("You do not have permission to do queries as a normal user! Please try login again!")
          }
          else println("You are a guest!")
        case _ => println("You pressed the wrong key!")
      }
    }
    def query:Unit= {
      var loop1 = true
      while (loop1) {
        println()
        println("============================================================")
        println()
        menu.printMenu()
        val input = readLine("Please enter your selection:\n").toInt
        input match {
          case 1 => Query1.Q1(spark, df)
          case 2 => Query2.query2(spark,df)
          case 3 => Query3.q3(spark, df)
          case 4 => Query4.q4(spark, df)
          case 5 => Query5.readBusinessData(spark,df)
          case 6 => Query6.query6(spark, df)
          case 7 => Query7.q7(spark, df)
          case 8 => loop1 = false
        }
      }
      println("Thanks for using Yelp query, good bye!")
    }
  }
}

package login
import Managers.SparkManager
import org.apache.spark.sql.functions._

import scala.io.StdIn.readLine
import scala.util.matching.Regex
import scala.io.StdIn.readLine
class UsersManagement {
  val sm = new SparkManager()
  sm.spark.sql("show tables").show
  sm.spark.sql("select * from usertable").show


  sm.spark.sql("CREATE TABLE IF NOT EXISTS usertable (username VARCHAR(30), password VARCHAR(50), userType VARCHAR(6));")
  def addAdmin:Unit={
    val validation = "(?=.*[0-9])(?=.*[a-z])(?=.*[A-Z]).{8,20}".r
    println("Please create admin username and password:")
    println("Password must have at least 1 number, 1 uppercase, 1 lowercase and between 8 to 20 characters")
    var user=readLine("Admin Username:")
    var password=readLine("Admin Password:")
    while(!password.matches(validation.toString)||user==null)
    {
      println("Password doesn't satisfy requirement, please try again..")
      user=readLine("Admin Username:")
      password=readLine("Admin Password:")
    }
      sm.spark.sql(s"INSERT INTO usertable VALUES ('$user','$password', 'admin')")
    println("Admin user added!")
  }

  def addNormal:Unit={
    val validation = "(?=.*[0-9])(?=.*[a-z])(?=.*[A-Z]).{8,20}".r
    println("Please create normal user username and password:")
    println("Password must have at least 1 number, 1 uppercase, 1 lowercase and between 8 to 20 characters")
    var user=readLine("Normal user Username:")
    var password=readLine("Normal user Password:")
    while(!password.matches(validation.toString)||user==null)
    {
      println("Password doesn't satisfy requirement, please try again..")
      user=readLine("Normal user Username:")
      password=readLine("Normal user Password:")
    }
    sm.spark.sql(s"INSERT INTO usertable VALUES ('$user','$password', 'normal')")
    println("Normal user added!")
  }
  def showTable:Unit={
    sm.spark.sql("select * from usertable").show
  }

  def prompt:(String,String)={
    val user=readLine("Please enter username:")
    val pass=readLine("PLease enter password:")
    (user,pass)
  }

  def checkPermission(input:(String,String)):String= {
   /*var found = " "
    var loop = true*/
    val table = sm.spark.sql(s"SELECT * FROM usertable where username ='${input._1}' and password = '${input._2}';")
    table.show()
    if(table.count()!= 1){
      //if the table returns anything other than one, than we know it didn't get a result
      println("User/password doesn't exist!")
      return null
    }
    //if the count is 1, then there was a match and we need to continue on.
    println("Login Successful")
    table.select("userType").first().mkString


    /*val collection = table.rdd.map(x=>(x.get(0),x.get(1),x.get(2))).collect
    val itr=collection.iterator
   // collection.foreach(println)
    while(itr.hasNext&&loop) {
      if (input._1 == itr.next()._1 && input._2 == itr.next()._2) {
        println("Login Successful!")
        found = itr.next()._3.toString
        loop = false
      }
    }
      if (found!=" ")
      found
      else {
        println("User/password doesn't exist!")
        null
      }*/
    }

}

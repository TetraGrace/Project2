package login
import Managers.SparkManager
import scala.io.StdIn.readLine

object Users {
  val sm = new SparkManager()

  class Login {
    def makeUserDB():Boolean = {
      val exist = sm.spark.catalog.tableExists("users")
      if(!exist){
        var userIn= ""
        do{
          userIn = readLine("No users DB found, would you like to make one? Y/n")
          userIn.toLowerCase() match {
            case "y" => println("Creating Database")
            case "n" => {
              println("Program cannot run without database. Quiting..."); return false;
            }
            case _ => println("please only enter y or n.")
          }}while (userIn != "y")
        //when it gets to this point, the user has entered y
        sm.spark.sql("CREATE TABLE users (username VARCHAR(30), password VARCHAR(50), userType VARCHAR(6));")
        println("You must create a admin user. please enter a user name and password. usernames must be greater than 8 characters, and less than 30 characters. Passwords must be greater than 8 characters and less than 50 characters.")
        var breaker = true
        while(breaker){
          val user = readLine("Username: ")
          val password = readLine("Password: ")
          if(password.length < 8 || password.length > 50 || user.length < 6 || user.length > 30) {
            breaker = true
            println("Password or Username is either too long or too short")
          } else {
            breaker = false
            sm.spark.sql(s"INSERT INTO users VALUES ('$user','$password', 'admin')")
          }
        }
        true
      } else true
    }

    def loginUser():String = {
      var tries = 0
      val maxTries = 3

      while (tries < maxTries){
        println("Please enter your username and password")
        val userName = readLine("Username: ")
        val pass = readLine("Password: ")

        val res = sm.spark.sql(s"SELECT * FROM users where username ='$userName' and password = '$pass';")
        if(res.count() != 1){
          //if there is no results, then there is no result from the query, there is no match for the username and password
          println("Username or password were incorrect. Please try again.")
          tries += 1
        } else {
          return res.select("userType").first().mkString
        }
      }
      return "none"
    }

    def addUser():Boolean = {
      println("Please enter the new users details.")
      println("Please enter a user name and password. usernames must be greater than 8 characters, and less than 30 characters. Passwords must be greater than 8 characters and less than 50 characters.")
      val username = readLine("Username: ")
      val password = readLine("Password: ")
      println("Please enter the User's permission level: 'admin' or 'normal'.")
      val permLevel = readLine("Permission Level: ")
      if(password.length < 8 || password.length > 50 || username.length < 6 || username.length > 30 ) {
        if(permLevel != "admin" && permLevel != "normal"){
          println("Permission Level entered incorrectly.")
          return false
        }
        println("Password or Username is either too long or too short.")
        false
      } else {
        sm.spark.sql(s"INSERT INTO users VALUES ('$username','$password', '$permLevel')")
        true
      }

    }
  }
}

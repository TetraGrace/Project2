package login

import scala.collection.mutable.ListBuffer

object MenuStuff {
  class Menu(options:List[MenuObject], name:String) {
    //map of all the map options. The int is in the position of the menu option starting at 0. The string is the return value for when its selected
    var menuOptions = options.toBuffer
    //the size of the menu, not including borders
    val menuSize = 35
    val menuName = name;

    private def printMenuLine(): Unit = {
      //prints a line that goes on the top of bottom of a menu
      var temp =""
      for(i<-0 to menuSize) temp +="-"
      println("+-"+ temp + "+")
    }

    private def printMenuOption(menObj: MenuObject): Unit = {
      //Prints out a section of the menu that is a set amount of characters long.

      var tempStr = "| "
      tempStr += menObj.id.toString + ">>" + menObj.returnVal
      while (tempStr.length < (menuSize - 1)) {
        tempStr += " "
      }
      tempStr += "|"
      println(tempStr)
    }

    private def printEmptyMenuLine(): Unit = {
      //prints and empty menu line to avoid confusion
      println("|                                                |")
    }

    def printMenu(): Unit = {
      //prints the entire menu with all options
      println("+-Options----------------------------------------+")
      menuOptions.foreach(m => printMenuOption(m))
      printMenuLine()
    }
    def selectOption(cho: Int):String = {
      return menuOptions(cho).returnVal;
    }
    def addMenuOption(item:MenuObject ):Unit={
      menuOptions +=item
    }
  }

  //this case class replaces the map and is just stores some data that is useful to menu
  case class MenuObject(id: Int, returnVal:String, itemDesc:String)
}

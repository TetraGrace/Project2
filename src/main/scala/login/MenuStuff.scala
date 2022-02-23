package login

import scala.collection.mutable.ListBuffer

object MenuStuff {
  class Menu(options:List[MenuObject], name:String) {
    //map of all the map options. The int is in the position of the menu option starting at 0. The string is the return value for when its selected
    var menuOptions = options.toBuffer
    //the size of the menu, not including borders
    val menuSize = 50
    val menuName = name;

    private def printMenuLine(): Unit = {
      //prints a line that goes on the top of bottom of a menu

      var temp ="+"
      for(i<-temp.length to menuSize-1) temp +="-"
      println(temp + "+")

    }

    private def printMenuOption(menObj: MenuObject): Unit = {
      //Prints out a section of the menu that is a set amount of characters long.

      var tempStr = "| "
      tempStr += menObj.id.toString + ">> " + menObj.itemDesc
      while (tempStr.length < (menuSize)) {

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

      var temp = "+-" + menuName
      for(i<-temp.length until menuSize) temp +="-"
      println(temp + "+")

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
  //Menu options are what the menuOptions are made of

  case class MenuObject(id: Int, returnVal:String, itemDesc:String)
}

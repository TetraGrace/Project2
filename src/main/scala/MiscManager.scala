import java.io.File
import scala.reflect.io.Directory

object MiscManager {

  val outputPath = new Directory(new File("D:\\IdeaProjects\\repo\\project1\\output"))



  //<editor-fold desc="Time Delay">

  def WaitForSeconds(milliseconds: Int): Unit = {
    Thread.sleep(milliseconds)
  }

  //</editor-fold>
}

/**
  * Created by guochenzhao on 2018/5/12.
  */

import java.awt.Toolkit._

object CountDownAndBeep {

  val timingMap = Map[Char, Tuple2[String, Int]](
    '\n' -> ("Enter" -> 15),
    ' ' -> ("Space" -> 45))
  val escAscii = 27
  val message = "*" * 5 + prepMessage + "*" * 5

  def prepMessage: String = {
    timingMap.iterator.map {
      case (char, (name, time)) =>
        s"$name for $time"
    }.mkString(", ")
  }

  def waitThenBeep(seconds: Int): Unit = {
    var stopFlag = false

    def sleepAndPrint(seconds: Int): Unit = {
      (0 until seconds).foreach { i =>
        println(seconds - i)
        Thread.sleep(1000)
      }
    }

    def beep: Unit = {
      val beep = getDefaultToolkit.beep _
      (0 until 3).foreach { _ =>
        beep.apply()
        Thread.sleep(1000)
      }
    }


    sleepAndPrint(seconds)
    beep
  }

  def main(args: Array[String]) = {
    while (true) {
      println(message)
      Console.in.read.toChar match {
        case c if escAscii == c.toInt => sys.exit
        case c if timingMap.contains(c) => waitThenBeep(timingMap(c)._2)
        case _ =>
      }
    }
  }
}


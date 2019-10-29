import scala.io.Source

/**
  * Created by guochenzhao on 2019/10/30.
  */
object AnalyzeLog {


  val path = "/Users/guochenzhao/Downloads/onlyThread128.csv"
  val LEAVE = "Leave"
  val READ = "read"
  val SEEK = "seek"
  val OPEN = "open"

  def findReadTime(): Unit = {
    var total: Double = 0
    var times = 0
    val iter = Source.fromFile(path).getLines()
    while (iter.hasNext) {
      val line1 = iter.next().substring(8)
      if (iter.hasNext) {
        val line2 = iter.next().substring(8)
        val predicateLine1 = line1.contains(READ) && !line1.contains(LEAVE)
        val predicateLine2 = line2.contains(READ) && line2.contains(LEAVE)
        if (predicateLine1 && predicateLine2) {
          times += 1
          try {
            val timeStart = line1.split("read:").apply(1).substring(1).replaceAll(",", "").toDouble
            val timeEnd = line2.split("read:").apply(1).substring(1).replaceAll(",", "").toDouble
            total += (timeEnd-timeStart)

          } catch {
            case e: Exception =>
              println(line1.split("read:").mkString("    "))
              println(line1.split("read:").mkString("    "))
          }
        }
      }
    }
    println(times)
    println(total)
  }

  def findSeekTime() = {
    var total: Double = 0
    var times = 0
    val iter = Source.fromFile(path).getLines()
    while (iter.hasNext) {
      val line1 = iter.next().substring(8)
      if (iter.hasNext) {
        val line2 = iter.next().substring(8)
        val predicateLine1 = line1.contains(SEEK) && !line1.contains(LEAVE)
        val predicateLine2 = line2.contains(SEEK) && line2.contains(LEAVE)

        if (predicateLine1 && predicateLine2) {
          times += 1
          try {
            val timeStart = line1.split("seek:").apply(1).substring(1).replaceAll(",", "").toDouble
            val timeEnd = if (line2.contains("seek2")) {
              line2.split("seek2:").apply(1).substring(1).replaceAll(",", "").toDouble
            } else {
              line2.split("seek:").apply(1).substring(1).replaceAll(",", "").toDouble
            }
            total += (timeEnd-timeStart)

          } catch {
            case e: Exception =>
              println(line1.split("seek:").mkString("    "))
              println(line1.split("seek:").mkString("    "))
          }
        }
      }
    }
    println(times)
    println(total)
  }

  def findOpenTime(): Unit = {
    var total: Double = 0
    var times = 0
    val lines = Source.fromFile(path).getLines().toSeq
    for (i <- (0 until lines.size)) {
      val line1 = lines(i).substring(8)
      if (i + 3 < lines.size) {
        val line2 = lines(i + 3).substring(8)
        val predicateLine1 = line1.contains(OPEN) && !line1.contains(LEAVE)
        val predicateLine2 = line2.contains(OPEN) && line2.contains(LEAVE)
        if (predicateLine1 && predicateLine2) {
          times += 1
          try {
            val timeStart = line1.split("open:").apply(1).substring(1).replaceAll(",", "").toDouble
            val timeEnd = line2.split("open:").apply(1).substring(1).replaceAll(",", "").toDouble
            total += (timeEnd-timeStart)

          } catch {
            case e: Exception =>
              println(line1.split("open:").mkString("    "))
              println(line1.split("open:").mkString("    "))
          }
        }
      }
    }
    println(times)
    println(total)
  }

  def main(args: Array[String]) = {
    findReadTime()
    findSeekTime()
findOpenTime()

  }
}

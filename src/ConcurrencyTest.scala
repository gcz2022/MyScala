import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}
import sys.process._
import scala.concurrent.ExecutionContext.Implicits.global

object ConcurrencyTest {
  val scale = 30
//  val formats = List("oap", "parquet")
  val formats = List("oap")
  val tables = List("store_sales")
  val filterColumn = "ss_item_sk"
  val indices = List("store_sales_ss_item_sk1_index", "store_sales_ss_ticket_number_index")
  val futures = mutable.ArrayBuffer[(Future[String], String, (String) => Unit)]()

  val REFRESH_INDEX = 1
  val DROP_INDEX = 2

  val SCAN_DATA = 1
  val INSERT_DATA = 2
  val DROP_DATA = 3

  val indexHintsMap = mutable.HashMap[Int, String]()
  indexHintsMap += REFRESH_INDEX -> "refresh index"
  indexHintsMap += DROP_INDEX -> "drop index"
  val dataHintsMap = mutable.HashMap[Int, String]()
  dataHintsMap += SCAN_DATA -> "scan data"
  dataHintsMap += INSERT_DATA -> "insert data"
  dataHintsMap += DROP_DATA -> "drop data"

  def addToFutures[T](func: => String, hints: String, assertion: (String) => Unit = (String) => {}) = {
    futures += ((Future(func), hints, assertion))
  }

  def addPrefix(cmd: String, database: String): Seq[String] = {
    Seq("beeline", "-u", s"jdbc:hive2://localhost:10000/$database", "-e", cmd)
  }

  def testDataAndIndexOperation(
    codeForData: Int,
    codeForIndex: Int,
    index: String,
    table: String,
    database: String,
    dataOperationHint: String,
    indexOperationHint: String) = {

    def scanDataCmd(index: String, table: String, database: String) = {
      val cmd = s"select $filterColumn from $table where $filterColumn < 20 order by $filterColumn"
      val res = addPrefix(cmd, database)
      println(s"running: $res")
      res
    }

    def insertDataCmd(table: String, database: String) = {
      def times24(x: Int) = Seq.fill(24)(x).mkString("(", ",", ")")
      val insertedData = (1 to 100).foldLeft("")((str: String, x: Int) => s"$str, ${times24(x)}").tail
      val cmd = s"insert into table $table values $insertedData"
      val res = addPrefix(cmd, database)
      println(s"running: $res")
      res
    }

    def dropDataCmd(table: String, database: String) = {
      val cmd = s"drop table $table"
      val res = addPrefix(cmd, database)
      println(s"running: $res")
      res
    }

    def showTableCmd(database: String) = {
      val cmd = s"show tables"
      val res = addPrefix(cmd, database)
      println(s"running: $res")
      res
    }

    def showIndexCmd(table: String, database: String) = {
      val cmd = s"show oindex from $table"
      val res = addPrefix(cmd, database)
      println(s"running: $res")
      res
    }

    // Compute first
    val scanDataRightAnswer = if (codeForData == SCAN_DATA) {
      scanDataCmd(index, table, database) !!
    } else {
      _
    }

    println(s"************************ " +
      s"Testing $indexOperationHint & $dataOperationHint ************************")

    codeForData match {
      case (SCAN_DATA, _) =>
        addToFutures(scanDataCmd(index, table, database) !!, dataOperationHint,
          (ans: String) => assert(ans == scanDataRightAnswer, "Bong! Scan answer wrong"))
      case (INSERT_DATA, _) =>
        addToFutures(insertDataCmd(table, database) !!, dataOperationHint)
      case DROP_DATA =>
        addToFutures(dropDataCmd(table, database) !!, dataOperationHint,
          (ans: String) => assert(! (showTableCmd(database) !!).contains(table), "Bong! Table not dropped"))
    }

    codeForIndex match {
      case REFRESH_INDEX =>
        addToFutures(refreshIndexCmd(index, table, database) !!, indexOperationHint)
      case DROP_INDEX =>
        addToFutures(dropIndexCmd(index, table, database) !!, indexOperationHint,
          (ans: String) => assert(! (showIndexCmd(table, database) !!).contains(index), "Bong! Index not dropped"))
    }

    waitForTheEndAndPrintResAndClear
  }

  def refreshIndexCmd(index: String, table: String, database: String) = {
    val cmd = s"""
                 |refresh oindex ${index} on ${table}
                 |""".stripMargin
    val res = addPrefix(cmd, database)
    println(s"running: $res")
    res
  }

  def dropIndexCmd(index: String, table: String, database: String) = {
    val cmd = s"""
       |drop oindex ${index} on ${table}
       |""".stripMargin
    val res = addPrefix(cmd, database)
    println(s"running: $res")
    res
  }

  def waitForTheEndAndPrintResAndClear = {
    futures.foreach {
      future =>
        Await.result(future._1, Duration.Inf)
        future._1.onComplete {
          case Success(value) => /* future._3 does assertion*/
            future._3(value)
            println(s"Assertion passed ${future._2}")
          case Failure(e) => println(s"Failed exec ${future._2}"); e.printStackTrace
        }
    }
    futures.clear()
  }

  def dropDataAndRefreshIndex(index: String, table: String, database: String) = {
    testDataAndIndexOperation(
      DROP_DATA, REFRESH_INDEX,
      index, table, database,
      s"drop table $table, database $database",
      s"Refresh index $index, table $table, database $database")
  }

  def refreshOrDropIndicesFromSameTable(indices: Seq[String], table: String, database: String) = {
    println(s"************************ " +
      s"Testing drop indicies from same table ************************")
    addToFutures(dropIndexCmd(indices(0), table, database) !!,
      s"Drop index ${indices(0)} of table $table, database $database")
    addToFutures(dropIndexCmd(indices(1), table, database) !!,
      s"Drop index ${indices(1)} of table $table, database $database")
    waitForTheEndAndPrintResAndClear
    rebuildIndex

    println(s"************************ " +
      s"Testing drop index and refresh index from same table ************************")
    addToFutures(dropIndexCmd(indices(0), table, database) !!,
      s"Drop index ${indices(0)} of table $table, database $database")
    addToFutures(refreshIndexCmd(indices(1), table, database) !!,
      s"Refresh index ${indices(1)} of table $table, database $database")
    waitForTheEndAndPrintResAndClear
    rebuildIndex

    println(s"************************ " +
      s"Testing refresh indicies from same table ************************")
    addToFutures(refreshIndexCmd(indices(0), table, database) !!,
      s"Refresh index ${indices(0)} of table $table, database $database")
    addToFutures(refreshIndexCmd(indices(1), table, database) !!,
      s"Refresh index ${indices(1)} of table $table, database $database")
    waitForTheEndAndPrintResAndClear
    rebuildIndex
  }

  def regenData = {
    addToFutures(regenDataScript !!, "Regen data")
    waitForTheEndAndPrintResAndClear
  }

  def rebuildIndex = {
    addToFutures(rebuildIndexScript !!, "Rebuild index")
    waitForTheEndAndPrintResAndClear
  }

  var rebuildIndexScript: String = _
  var regenDataScript: String = _

  def main(args: Array[String]) = {

    rebuildIndexScript = args(0)
    regenDataScript = args(1)

    // Default to test all cases
    val (testIndexOpsSet, testDataOpsSet) = if (args.length > 2) {
      (Set(args(2).toInt), Set(args(3).toInt))
    } else {
      (Set(REFRESH_INDEX, DROP_INDEX), Set(SCAN_DATA, INSERT_DATA, DROP_DATA))
    }

    for (format <- formats) {
      val database = s"${format}tpcds${scale}"
      for (table <- tables) {
        val index = indices(0)
//          refreshOrDropIndicesFromSameTable(indices, table, database)
//        dropDataAndRefreshIndex(index, table, database)
        for (indexOps <- testIndexOpsSet) {
          for (dataOps <- testDataOpsSet) {
            val indexHint = indexHintsMap(indexOps)
            val dataHint = dataHintsMap(dataOps)
            testDataAndIndexOperation(dataOps, indexOps, index, table, database, dataHint, indexHint)
            if (indexOps == DROP_INDEX) {
              println("Rebuilding index")
              rebuildIndex
            }
            if (dataOps == DROP_DATA) {
              println("Regening data")
              regenData
              rebuildIndex
            }
          }
        }
      }
    }
  }
}

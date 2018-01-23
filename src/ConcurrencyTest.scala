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
  val indices = List("store_sales_ss_item_sk1_index", "store_sales_ss_ticket_number_index")
  val futures = mutable.ArrayBuffer[(Future[Int], String)]()

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

  def addToFutures[T](func: => Int, hints: String) = {
    futures += Future {
      func
    } -> hints
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
      val cmd = s"select * from $table where $index < 200"
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

    def refreshIndexCmd(index: String, table: String, database: String) = {
      val cmd = s"""
                   |refresh oindex ${index} on ${table}
                   |""".stripMargin
      val res = addPrefix(cmd, database)
      println(s"running: $res")
      res
    }

    codeForData match {
      case SCAN_DATA =>
        addToFutures(scanDataCmd(index, table, database) !, dataOperationHint)
      case INSERT_DATA =>
        addToFutures(insertDataCmd(table, database) !, dataOperationHint)
      case DROP_DATA =>
        addToFutures(dropDataCmd(table, database) !, dataOperationHint)
    }

    codeForIndex match {
      case REFRESH_INDEX =>
        addToFutures(refreshIndexCmd(index, table, database) !, indexOperationHint)
      case DROP_INDEX =>
        addToFutures(dropIndexCmd(index, table, database) !, indexOperationHint)
    }

    waitForTheEndAndPrintResAndClear
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
          case Success(value) => println(s"Successfully exec ${future._2}")
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

  def dropIndicesFromSameTable(indices: Seq[String], table: String, database: String) = {
    addToFutures(dropIndexCmd(indices(0), table, database) !,
      s"Drop index ${indices(0)} of table $table, database $database")
    addToFutures(dropIndexCmd(indices(1), table, database) !,
      s"Drop index ${indices(1)} of table $table, database $database")
    waitForTheEndAndPrintResAndClear
  }

  def regenData = {
    regenDataScript !
  }

  def rebuildIndex = {
    rebuildIndexScript !
  }

  var rebuildIndexScript: String = _
  var regenDataScript: String = _

  def main(args: Array[String]) = {

    rebuildIndexScript = args(0)
    regenDataScript = args(1)

    for (format <- formats) {
      val database = s"${format}tpcds${scale}"
      for (table <- tables) {
        val index = indices(0)
//        dropIndicesFromSameTable(indices, table, database)
//        dropDataAndRefreshIndex(index, table, database)
        for (indexOps <- Seq(REFRESH_INDEX, DROP_INDEX)) {
          for (dataOps <- Seq(SCAN_DATA, INSERT_DATA, DROP_DATA)) {
            val indexHint = indexHintsMap(indexOps)
            val dataHint = dataHintsMap(dataOps)
            println(s"********** Testing $indexHint & $dataHint **********")
            testDataAndIndexOperation(dataOps, indexOps, index, table, database, dataHint, indexHint)
            if (indexOps == DROP_INDEX) {
              rebuildIndex
            }
            if (indexOps == DROP_DATA) {
              regenData
              rebuildIndex
            }
          }
        }
      }
    }
  }
}

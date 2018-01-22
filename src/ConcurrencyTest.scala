import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}
import sys.process._
import scala.concurrent.ExecutionContext.Implicits.global

object ConcurrencyTest {
  val scale = 30
  val formats = List("oap", "parquet")
  val tables = List("store_sales")
  val indices = List("store_sales_ss_item_sk1_index", "store_sales_ss_ticket_number_index")

  def addPrefix(cmd: String, database: String) = {
    s"""
       |beeline -u "jdbc:hive2://localhost:10000" -e
       |"use ${database};
       |${cmd};"
       |""".stripMargin

  }

  def dropIndexCmd(index: String, table: String, database: String) = {
    val cmd = s"""
       |drop oindex ${index} from ${table}
       |""".stripMargin
    addPrefix(cmd, database)
  }

  def main(args: Array[String]) = {
    val indexOperationFutures = mutable.ArrayBuffer[(Future[Int], String)]()
    val dataOperationFutures = mutable.ArrayBuffer[(Future[Int], String)]()
    for (format <- formats) {
      val database = s"${format}tpcds${scale}"
      for (table <- tables) {
        for (index <- indices) {
          indexOperationFutures += Future {
            dropIndexCmd(index, table, database) !
          } -> s"drop index $index of table $table, database $database"
        }
      }
    }
    indexOperationFutures.foreach {
      future =>
        Await.result(future._1, Duration.Inf)
        future._1.onComplete {
        case Success(value) => println(s"Successfully exec ${future._2}")
        case Failure(e) => e.printStackTrace
      }
    }
    dataOperationFutures.foreach {
      future =>
        Await.result(future._1, Duration.Inf)
        future._1.onComplete {
        case Success(value) => println(s"Successfully exec ${future._2}")
        case Failure(e) => println(s"Failed exec ${future._2}"); e.printStackTrace
      }
    }
  }
}
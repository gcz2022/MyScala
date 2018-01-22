import scala.collection.mutable
import scala.concurrent.Future
import sys.process._

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
    val indexOperationFutures = mutable.ArrayBuffer[Future[Int]]()
    val dataOperationFutures = mutable.ArrayBuffer[Future[Int]]()
    for (format <- formats) {
      val database = s"${format}tpcds${scale}"
      for (table <- tables) {
        for (index <- indices) {
          indexOperationFutures += Future {
            dropIndexCmd(index, table, database) !
          }
        }
      }
    }
  }
}
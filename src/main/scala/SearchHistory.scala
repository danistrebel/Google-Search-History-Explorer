import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.io.File
import org.json4s._
import org.json4s.jackson.JsonMethods._

case class Query(timestamp: Long, query: String)

object SearchHistory extends App {
  val searchHistory = new File(args(0)).listFiles()
  val conf = new SparkConf().setAppName("Google Search History").setMaster("local[2]")
  val sc = new SparkContext(conf)
  val jsonInput = searchHistory.map(file2JsonInput(_)).map(parse(_))
  val history = sc.makeRDD(jsonInput)
  val queries = history.map(_ \ "event").flatMap({
    case JArray(arr) => {
      arr.map(wrapped => {

        val unwrapped = wrapped \ "query"
        val time = unwrapped \ "id" \ "timestamp_usec"

        val query = unwrapped \ "query_text"

        val queryString = query match {
          case JString(s) => s
          case _ => throw new RuntimeException("expected string")
        }

        val timestamp = time match {
          case JString(s) => s.toLong
          case JArray(JString(s)::xs) => s.toLong
          case _ => throw new RuntimeException("Expected Long in " + time)
        }

        Query(timestamp, queryString)
      })
    }
    case _ => throw new RuntimeException("unexpected format")
  })

  val terms = queries.flatMap(_.query.split(" ")).map(word => (word, 1))
    .reduceByKey(_ + _)


  //Top 25 search terms
  terms.sortBy(_._2, false).take(25).foreach(println(_))
}

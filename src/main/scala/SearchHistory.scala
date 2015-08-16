import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.io.File
import org.joda.time.DateTime
import org.json4s._
import org.json4s.jackson.JsonMethods._

case class Query(timestamp: Long, query: String) {

  lazy val dateTime = {
    val millis = timestamp/1000
    new DateTime(millis)
  }
  def year = dateTime.getYear
  def month = dateTime.getMonthOfYear
  def yearMonth = year * 100 + month //Format: YYYYMM
  def weekday = dateTime.getDayOfWeek
  def terms = query.split(" ").map(_.toLowerCase)
}

object Query {
  def apply(json: JValue): Query = {
    val unwrapped = json \ "query"
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
  }
}

object SearchHistory extends App {
  val searchHistory = new File(args(0)).listFiles()
  val conf = new SparkConf().setAppName("Google Search History").setMaster("local[2]")
  val sc = new SparkContext(conf)
  val jsonInput = searchHistory.map(file2JsonInput(_)).map(parse(_))
  val history = sc.makeRDD(jsonInput)

  //Parse json into query objects
  val queries = history.map(_ \ "event").flatMap({
    case JArray(arr) => { arr.map(queryObject => Query(queryObject)) }
    case _ => throw new RuntimeException("unexpected input format")
  }).cache

  //Top 25 search terms
  val terms = queries.flatMap(_.terms).map(word => (word, 1)).reduceByKey(_ + _)
  terms.sortBy(_._2, false).take(25).foreach(println(_))

  //Query count by month
  val monthlyQueries = queries.map(q => (q.yearMonth, 1)).reduceByKey(_ + _)
  monthlyQueries.sortBy(_._1).collect().foreach(println(_))

  //Query count by weekday
  val weekdayQueries = queries.map(q => (q.weekday, 1)).reduceByKey(_ + _)
  weekdayQueries.sortBy(_._1).collect().foreach(println(_))

  //Top 10 per Year
  val yearQueries = queries.flatMap(q => q.terms.map(term => (q.year, term)))
  val topYearQueries = yearQueries.groupByKey().mapValues(_.groupBy(identity).mapValues(_.size).toSeq.sortBy(_._2).takeRight(10))
  topYearQueries.sortBy(_._1).collect().foreach(println(_))
}

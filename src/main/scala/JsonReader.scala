import org.apache.spark.sql.SparkSession
import org.json4s.{NoTypeHints, _}
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization


object JsonReader extends App {
  case class Winemag(id: Option[Int],
                     country: Option[String],
                     points: Option[Int],
                     price: Option[Double],
                     title: Option[String],
                     variety: Option[String],
                     winery: Option[String])
  implicit val formats = Serialization.formats(NoTypeHints)

  val spark = SparkSession.builder().appName("json_reader_Yakovlev").master("local").getOrCreate()
  val sc = spark.sparkContext
  val filename = args(0)
  //val filename = "D:\\json_reader_Yakovlev\\winemag-data-130k-v2.json"

  val winemags = sc.textFile(filename)
    .map(s => parse(s).extract[Winemag])
    .foreach { println }

}

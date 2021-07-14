package wtf
import org.apache.spark.sql.SparkSession

trait SnowF {

  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("lull")
    .getOrCreate()

  var sfOptions = Map(
    "sfURL" -> "",
    "sfAccount" -> "",
    "sfUser" -> "",
    "sfPassword" -> "",
    "sfDatabase" -> "",
    "sfSchema" -> "PUBLIC",
    "sfRole" -> "ACCOUNTADMIN"
  )
}

package foo
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}


object Fu extends App with SnowF {



  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  val simpleData = Seq(("James","Sales",3000),
    ("Michael","Sales",4600),
    ("Robert","Sales",4100),
    ("Maria","Finance",3000),
    ("Raman","Finance",3000),
    ("Scott","Finance",3300),
    ("Jen","Finance",3900),
    ("Jeff","Marketing",3000),
    ("Kumar","Marketing",2000)
  )
  val df = simpleData.toDF("name","department","salary")
  df.show()


  val df1 = Seq(("bhai wah", "20"),("lol", "30")).toDF("name","age")

//  df1.write
//    .format("snowflake")
//    .options(sfOptions)
//    .option("dbtable", "hola")
//    .mode(SaveMode.Append)
//    .save()

  val dfe: DataFrame = spark.read
    .format("net.snowflake.spark.snowflake") // or just use "snowflake"
    .options(sfOptions)
    .option("dbtable", "hola")
    .load()

  dfe.show(false)
}
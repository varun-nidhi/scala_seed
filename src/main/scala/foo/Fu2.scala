package foo


import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}


object Fu2 extends App  with SnowF {



  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  val simpleData = Seq(("James","Sales",Map(3 -> "2")),
    ("Jeff","Marketing",Map(1 -> "hi"))
  )


  val df = simpleData.toDF("name","department","salary")
  df.show()
  df.printSchema()


  df.createOrReplaceTempView("lobby")
  spark.sql("select salary[3] from lobby").show()


//     df.write
//       .format("snowflake")
//       .options(sfOptions)
//       .option("dbtable", "withmap")
//       .mode(SaveMode.Append)
//       .save()

    val dfe: DataFrame = spark.read
   .format("net.snowflake.spark.snowflake") // or just use "snowflake"
   .options(sfOptions)
   .option("dbtable", "withmap").schema(df.schema)
   .load()
 dfe.printSchema()
 dfe.show(false)

  dfe.createOrReplaceTempView("sakinaka")
  spark.sql("select salary[3] from sakinaka").show()

}



package foo

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

object Job2 extends App {


  val spark = SparkSession.builder().appName("Job2")
    .getOrCreate();

  spark.sparkContext.setLogLevel("ERROR")

  val simpleData = Seq(Row("James", "Sales", Row(Map(1 -> "yo"), "all is well", Array(3000, 201), Row(23, "lol"))),
    Row("Jeff", "Marketing", Row(Map(3 -> "yu", 4 -> "pp"), "sab moh maya hai", Array(3000, 200), Row(50, "gadhe")))
  )
  val value: RDD[Row] = spark.sparkContext.parallelize(simpleData)
  val simpldedf = spark.createDataFrame(value, StructType(Array(StructField("name", StringType), StructField("department", StringType),
    StructField("salary", StructType(Array(StructField("a", MapType(IntegerType, StringType)), StructField("b", StringType),
      StructField("c", ArrayType(IntegerType)), StructField("d", StructType(Array(StructField("l", IntegerType), StructField("m", StringType)))
      )))))))

  simpldedf.write
    .mode(SaveMode.Overwrite)
    .save("dbfs:/mnt/data/core/output_par")


  val dfe: DataFrame = spark.read.parquet("dbfs:/mnt/data/core/output_par")
  dfe.show()

  dfe.createOrReplaceTempView("sakinaka")
  dfe.printSchema()
  dfe.show(false)
  spark.sql("select name, department, salary.b " +
    ", salary.c[0], salary.d.l" +
    " from sakinaka").show()


  var sfOptions = Map(
    "sfURL" -> "",
    "sfAccount" -> "",
    "sfUser" -> "",
    "sfPassword" -> "",
    "sfDatabase" -> "",
    "sfSchema" -> "PUBLIC",
    "sfRole" -> "ACCOUNTADMIN"
  )


  simpldedf.write
    .format("snowflake")
    .options(sfOptions)
    .option("dbtable", "sakinaka")
    .mode(SaveMode.Overwrite)
    .save()

  val dfes: DataFrame = spark.read
    .format("net.snowflake.spark.snowflake") // or just use "snowflake"
    .options(sfOptions)
    .option("dbtable", "sakinaka")
    .load()


  dfes.createOrReplaceTempView("sakinaka_snowflake")
  dfes.printSchema()
  dfes.show(false)
  spark.sql("select name, department, salary from sakinaka_snowflake" )

}

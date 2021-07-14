package wtf

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode, SparkSession}


object Fu1 extends App with SnowF {



  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  val simpleData = Seq(Row("James","Sales",Array(3000,201)),
    Row("Jeff","Marketing",Array(3000,200))
  )
  private val value: RDD[Row] = spark.sparkContext.parallelize(simpleData)
  private val frame: DataFrame = spark.createDataFrame(value, StructType(Array(StructField("name", StringType), StructField("department", StringType), StructField("salary", ArrayType(IntegerType)))))


  //  val df = simpleData.toDF("name","department","salary")
  //  df.show()
  //  df.printSchema()
  frame.createOrReplaceTempView("frame")

  spark.sql("select salary[0] from frame").show()
  //  frame.select("salary").show()


  frame.write
    .format("snowflake")
    .options(sfOptions)
    .option("dbtable", "witharray")
    .mode(SaveMode.Overwrite)
    .save()

  val dfe: DataFrame = spark.read
    .format("net.snowflake.spark.snowflake") // or just use "snowflake"
    .options(sfOptions)
    .option("dbtable", "witharray").schema(frame.schema)
    .load()
  dfe.printSchema()
  dfe.show(false)
  dfe.select("salary").show()
  dfe.createOrReplaceTempView("dfe")
  spark.sql("select salary[0] from dfe").show()

}
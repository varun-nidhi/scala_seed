package wtf

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{ArrayType, IntegerType, MapType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode, SparkSession, types}


object Fu3 extends App with SnowF {


  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  val simpleData = Seq(Row("James","Sales",Row(Map(1 -> "yo"), "all is well", Array(3000,201),Row ( 23, "lol"))),
    Row("Jeff","Marketing",Row(Map(3 -> "yu", 4 -> "pp" ), "sab moh maya hai", Array(3000,200),Row ( 50, "gadhe")))
  )
   val value: RDD[Row] = spark.sparkContext.parallelize(simpleData)
  val simpldedf = spark.createDataFrame(value, StructType(Array(StructField("name", StringType), StructField("department", StringType),
    StructField("salary",StructType(Array(StructField("a",MapType(IntegerType,StringType)), StructField("b", StringType),
    StructField("c",ArrayType(IntegerType)), StructField( "d", StructType(Array (StructField("l",IntegerType), StructField("m", StringType)))
  )))))))

  simpldedf.show()
  simpldedf.printSchema()

simpldedf.createOrReplaceTempView("lobby")
  spark.sql("select name, department, salary.b " +
    //    ", salary.c[0], salary.d.l" +
    " from lobby").show()

  simpldedf.write
        .format("snowflake")
        .options(sfOptions)
        .option("dbtable", "withstruct_explicitschema")
        .mode(SaveMode.Overwrite)
        .save()

  val dfe: DataFrame = spark.read
    .format("net.snowflake.spark.snowflake") // or just use "snowflake"
    .options(sfOptions)
    .option("dbtable", "withstruct_explicitschema").schema(simpldedf.schema)
    .load()

  dfe.createOrReplaceTempView("sakinaka")
  dfe.printSchema()
  dfe.show(false)
  spark.sql("select name, department, salary.b " +
//    ", salary.c[0], salary.d.l" +
    " from sakinaka").show()
}
package wtf

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{ArrayType, IntegerType, MapType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode, SparkSession, types}

object Notebook extends App {



  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("lull")
    .getOrCreate();

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
"sfURL" -> "https://tn61475.central-us.azure.snowflakecomputing.com/",
"sfAccount" -> "tn61475",
"sfUser" -> "varun30",
"sfPassword" -> "Varun@snow30",
"sfDatabase" -> "letsnow",
"sfSchema" -> "PUBLIC",
"sfRole" -> "ACCOUNTADMIN"
)


simpldedf.write
.format("snowflake")
.options(sfOptions)
.option("dbtable", "withstruct_explicitschema")
.mode(SaveMode.Overwrite)
.save()

val dfes: DataFrame = spark.read
.format("net.snowflake.spark.snowflake") // or just use "snowflake"
.options(sfOptions)
.option("dbtable", "withstruct_explicitschema").schema(simpldedf.schema)
.load()


dfes.createOrReplaceTempView("sakinaka_snowflake")
dfes.printSchema()
dfes.show(false)
spark.sql("select name, department, salary.b " +
//    ", salary.c[0], salary.d.l" +
" from sakinaka_snowflake").show()

}
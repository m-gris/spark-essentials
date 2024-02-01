package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode

object DataSources extends App {
  
  val spark = SparkSession.builder()
    .appName("Data Sources and Formats")
    .config("spark.master", "local")
    .getOrCreate()

  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))

  /*
   READING DATAFRAMES 
    - format 
    - schema (explicit or infered)
    - options 
   */
  
  val carsDF = spark.read
    .format("json")
    .schema(carsSchema)
    .option("mode", "failFast") // dropMalformed, permissive (default)
    .option("path", "src/main/resources/data/cars.json")
    .load()

  // alternative way: OPTIONMAP
  val cardsDFWithOptionMap = spark.read
    .format("json")
    .schema(carsSchema)
    .options(Map(
      "mode" -> "dropMalformed", 
      "path" -> "src/main/resources/data/cars.json", 
      ))
    .load()
  // BENEFIT: unsing the Map allows to compute options dynamically, at runtime.
  

  /* WRITING DATAFRAMES
    - format
    - save mode = overwrite, append, ignore, errorIfExists
    - path 
    - zero or more options
   */
  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .option("path", "src/main/resources/data/cars_copy.json")
    .save()


  // JSON flags / options 
  {
  // 
  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    // NOTA: here we change from StringType to DateType
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))

  spark.read
    // commented... look below... 
    // .format("json") 
    .schema(carsSchema)
    .option("dateFormat", "YYYY-MM-dd") // nota dd not DD
    .option("allowSingleQuotes", "true")
    .option("compression", "uncompressed") // bzip2, gzip, lz4, snappy, deflate
    // .json()   is equivalent to .format('json') + load()
    .json("src/main/resources/data/cars.json")
  }


  // CSV 
  val stockSchema = StructType(Array(
    StructField("symbol", StringType), 
    StructField("date", DateType), 
    StructField("price", DoubleType), 
    ))

  spark.read
    .schema(stockSchema)
    .option("dateFormat", "YYYY-MM-dd") 
    .option("header", "true") // should match the schema's field
    .option("sep", ",") 
    .option("nullValue", "") 
    .csv("src/main/resources/data/stocks.csv")

  // PARQUET - The default for Spark 
  carsDF.write
    // we do not need to specify the format
    // .format("parquet")
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars.parquet")


  // TEXT FILES
  val textDF = spark.read.text("src/main/resources/data/sample_text.txt")
  textDF.show()

  // READING FROM A REMOTE DATABASE 
  val dbDriver =  "org.postgresql.Driver"
  val dbUrl =  "jdbc:postgresql://localhost:5432/rtjvm"
  val dbUser =  "docker"
  val dbPassword =  "docker"

  val employees = spark.read
    .format("jdbc")
    .option("driver", dbDriver)
    .option("url", dbUrl)
    .option("user", dbUser)
    .option("password", dbPassword)
    .option("dbtable", "public.employees")
    .load()

  employees.show()


  val moviesData = "src/main/resources/data/movies.json"

  val movies = spark.read.json(moviesData) // inferSchema is true by default

  movies.write
    .option("sep", "\t")
    .option("header", "true")
    .mode(SaveMode.Overwrite)
    .csv("src/main/resources/data/exo-outputs/movies.csv")

  movies.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/exo-outputs/movies.parquet")

  movies.write
    .format("jdbc")
    .mode(SaveMode.Overwrite)
    .option("driver", dbDriver)
    .option("url", dbUrl)
    .option("user", dbUser)
    .option("password", dbPassword)
    .option("dbtable", "public.movies")
    .save()
    
}

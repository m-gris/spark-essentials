package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

object DataframeBasics extends App {

  println("Hello DATAFRAMES")

  // our ENTRY POINT for READING & WRITING DFs
  val spark = SparkSession.builder() 
    .appName("DataFramesBasics")
    .config("spark.master", "local") // key - value 
    // nota: 1 config at a time... 
    // .config("another.key", "another.value") 
    .getOrCreate()

  val carsDataPath = "/Users/marc/Documents/DATA_PROG/SCALA/rock-the-jvm/spark-essentials/src/main/resources/data/cars.json"
  val firstDF = spark.read
    .format("json")
    .option("inferSchema", "true") // k-v like .config() for the session
    .load(carsDataPath)


  firstDF.show() // shows the first 20 lines 
  firstDF.printSchema() // +/- like pandas df.infos (col name - type - nullable)
  firstDF.take(10).foreach(println)
  //
  // IN PRACTICE: it is preferable to build the schema manually 
  // instead of letting spark infer the schema !!!
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
  // nota StructField / StructType, StringType, DoubleType etc... 
  // are all from  org.apache.spark.sql.types

  val carsDFWithSchema = spark.read
    .format("json")
    .schema(carsSchema)
    .load(carsDataPath)

  
  // creating ROWS by hand
  // Nota: a Row can contain ANYTHING
  // i.e rows are UNSTRUCTURED
  val myRow = Row("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA")

  // creating a DF manually from a SEQUENCE OF TUPLES
  val cars = Seq(
    ("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA"),
    ("buick skylark 320",15.0,8L,350.0,165L,3693L,11.5,"1970-01-01","USA"),
    ("plymouth satellite",18.0,8L,318.0,150L,3436L,11.0,"1970-01-01","USA"),
    ("amc rebel sst",16.0,8L,304.0,150L,3433L,12.0,"1970-01-01","USA"),
    ("ford torino",17.0,8L,302.0,140L,3449L,10.5,"1970-01-01","USA"),
    ("ford galaxie 500",15.0,8L,429.0,198L,4341L,10.0,"1970-01-01","USA"),
    ("chevrolet impala",14.0,8L,454.0,220L,4354L,9.0,"1970-01-01","USA"),
    ("plymouth fury iii",14.0,8L,440.0,215L,4312L,8.5,"1970-01-01","USA"),
    ("pontiac catalina",14.0,8L,455.0,225L,4425L,10.0,"1970-01-01","USA"),
    ("amc ambassador dpl",15.0,8L,390.0,190L,3850L,8.5,"1970-01-01","USA")
  )

  val manualCarsDF = spark.createDataFrame(cars)
  // schema auto-infered
  // but no column names (numbers instead of names)
  manualCarsDF.show()
  manualCarsDF.printSchema()
  
  // NOTA: schema are only applicable to DFs not to ROWS !!!!

  // create DFs with implicits
  import spark.implicits._
  // nota... yes... we are importing from our spark session !!!! 
  // TODO CHECK / DIG INTO THIS... 

  val carsDFWithImplicits = cars.toDF(
    // here we give the name of the cols / fields 
    // corresponding to the value in the tuples
    "Name", "MPG", "Cylinders", "Displacement", 
    "Horse Power", "Weight", "Accelaration", "Year", "Country"
    )

  carsDFWithImplicits.printSchema()

  
  // EXO
  // Create MANUALLY a DF of smarphones (make, model, megapixels)
  val phones = Seq(
    ("Apple", "iPhone 14", 2022, 18),
    ("Apple", "iPhone 15", 2023, 22),
    ("Samsung", "Galaxy Note 3", 2023, 24)
    ) 
  
  val phonesDF = phones.toDF("make", "model", "year", "megapixels")
  phonesDF.show()


  val moviesDF = spark.read
    .format("json")
    .option("inferSchema","true")
    .load("/Users/marc/Documents/DATA_PROG/SCALA/rock-the-jvm/spark-essentials/src/main/resources/data/movies.json")

  moviesDF.show()
  moviesDF.printSchema()
  println(s"The moviesDF has ${moviesDF.count()} rows")

}

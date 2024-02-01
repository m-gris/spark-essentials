package part2dataframes 

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, column, expr}

object ColumnsAndExpressions extends App {

  val spark = SparkSession.builder()
    .appName("DF columns & expressions")
    .config("spark.master", "local")
    .getOrCreate()

  val cars = spark.read.json("src/main/resources/data/cars.json")

  // cars.show()

  val carNamesCol = cars.col("Name") // plain scala object with no data inside !!!

  // Selecting
  // aka   PROJECTING
  val carNames = cars.select(carNamesCol) // returns a new DF


  // carNames.show()

  // selecting multiple columns 
  import spark.implicits._
  cars.select(
    // instead of typing 
    cars.col("Name"),
    // we can save a few keystrokes with the `col` function 
    col("Acceleration"), 
    // or the `column` function if we want something more explicit
    column("Weight_in_lbs"),
    // we can also do more fancy things with `implicits`
    'Year,  // `Scala Symbol`, auto-converted to a column
    $"Horsepower", // fancier interpolated string, also return a columns object
    expr("Origin") // EXPRESSION

  )


  // or will plain column names 
  // cannot be mixed with the above
  // for simple cases only
  cars.select("Name", "Year") 


  // EXPRESSIONS
  // the simplest expression
  val carWeightInPounds = cars.col("Weight_in_lbs")
  // the column object returns is a sub-type of an expression

  val carWeightInKilograms = cars.col("Weight_in_lbs") / 2.2
  // also returns a column object, describing a TRANSFORMATION

  val carWeightInMetricTons = cars.col("Weight_in_lbs") / 2204.62
  cars.select(
    'Name,
    carWeightInPounds,
    carWeightInKilograms, // col name will be `Weight_in_lbs / 2.2`)
    carWeightInMetricTons.as("Weight in Metric Tons"),
    expr("Weight_in_lbs / 2000").as("Weight in US Tons")
  )
  // .show()


  // short hand `.selectExpr()` 
  cars.selectExpr( // we can pass the expression strings directly
    "Name", 
    "Weight_in_lbs", 
    "Weight_in_lbs / 2.2"
    )
    // .show()


  // DF PROCESSING 
  //
  // Adding a new column
  var newColName = "Weight_in_kg"
  val newColExpression = col("Weight_in_lbs") / 2.2
  cars.withColumn(newColName, newColExpression)
      // .show()

  // renaming a column
  cars.withColumnRenamed("Weight_in_lbs", "Weight_in_pounds")
      // .show()


  // Removing a column 
  cars.withColumnRenamed("Weight_in_lbs", "Weight_in_pounds")
      .drop("Cylinders") 
      // NOTA BENE
      .drop("NonExistingColumn") // will not raise an error 
      // .show()

  // Filtering 
  // NOTA BENE: special column operators
  val isNotUSACar = col("Origin") =!= "USA" 
  val isUSACar = col("Origin") === "USA" 
  // cars.filter(isNotUSACar).show()
  // cars.filter(isUSACar).show()
  // equivalent to 
  // cars.where(isNotUSACar).show()
  // cars.where(nonUSA).show()

  // we can also directly pass an expression string
  cars.filter("Origin != 'USA'")

  // we can also CHAIN filters
  val powerfulAmericanCars = cars.filter("Origin == 'USA'")
                                .filter(col("Horsepower") > 150)
  // powerfulAmericanCars.show()

  // this could also be done by chaining expressions in the filter
  val isPowerful = col("Horsepower") > 150
  // cars.filter(isUSACar.and(isPowerful)).show()
  // NOTA: `and` can also be used with infix notation
  cars.filter(isUSACar and isPowerful)
  // We could also do 
  // cars.filter("Origin == 'USA' and Horsepower > 150").show()


  // UNIONING - i.e => adding more rows
  val moreCars = spark.read.json("src/main/resources/data/more_cars.json")
  val allCars = cars.union(moreCars) // works because they have the SAME SCHEMA


  // DISTINCT values
  val allCountries = allCars.select("Origin").distinct()
  // allCountries.show()

  
  val movies = spark.read.json("src/main/resources/data/movies.json")
  println("#" * 50)
  println("MOVIES DF COLUMNS:")
  movies.columns.sorted.foreach(println)
  println("#" * 50)

  // EXERCICES - As many way as possible for
  // SELECTING 2 COLUMNS
  movies.select("Title", "Release_Date")
  movies.select(col("Title"), column("Release_Date"))
  movies.select('Title, $"Release_Date")
  movies.select(movies.col("Title"), expr("Release_Date"))


  // COMPUTE TOTAL GROSS FOR EACH MOVIE
  val moviesProfit = col("US_Gross") + col("Worldwide_Gross")
  movies.select(col("Title"), moviesProfit)
  movies.selectExpr("Title", "US_Gross + Worldwide_Gross as TOTAL_GROSS")

  movies.select("Title", "US_Gross", "Worldwide_Gross")
        .withColumn("TOTAL_GROSS", col("US_Gross") + col("Worldwide_Gross"))
        .show()

  // SELECT GOOD COMEDIES 
  val isComedy = col("Major_Genre").isin("Black Comedy","Romantic Comedy","Comedy")
  val isGoodMovie = col("Rotten_Tomatoes_Rating") > 80
  movies.filter(isComedy and isGoodMovie)
  movies.where(isComedy and isGoodMovie)
  movies.where(isComedy).where(isGoodMovie)

  movies.select("Title", "Rotten_Tomatoes_Rating")
        .where("Major_Genre == 'Comedy' and Rotten_Tomatoes_Rating > 80")
        .show()
  



}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkJSONExample {
  def main(args: Array[String]): Unit = {



    val spark = SparkSession.builder
      .appName("Spark JSON Example")
      .config("spark.master", "local")
      .getOrCreate()

    import  spark.implicits._

    // Define the path to the JSON file
    val jsonFilePath = "/Users/vinodh/datasets/people.json"

    // Reading JSON from a file
    val peopleDF = spark.read.json(jsonFilePath)

    // Show the original DataFrame
    println("Original DataFrame:")
    peopleDF.show()

    // Transform the DataFrame: filter by age and select necessary columns
    val filteredDF = peopleDF.filter($"age" > 26).select("name", "age", "city")

    // Show the filtered DataFrame
    println("Filtered DataFrame:")
    filteredDF.show()

    // Define the path to the output JSON file
    val outputPath = "/Users/vinodh/datasets/filtered_people.json"

    // Write the DataFrame to a JSON file
    filteredDF.write.mode("overwrite").json(outputPath)

    // Stop the Spark session
    spark.stop()
  }
}

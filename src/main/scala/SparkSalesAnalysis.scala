import org.apache.spark.sql.SparkSession
import org.apache
import org.apache.spark.sql.functions.sum

object SparkSalesAnalysis {
  def main(args: Array[String]): Unit = {
    // Initialize SparkSession
    val spark = SparkSession.builder()
      .appName("Sales Analysis")
      .master("local[*]")  // Use local mode
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    // Read data from a CSV file
    val filePath = "/Users/vinodh/datasets"  // Update this path
    val dataDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(filePath)

    // Show the initial data
    dataDF.show()


    // Process the data: Calculate total sales per product
    val salesTotal = dataDF.groupBy("productID")
      .agg(sum("amount").alias("total_sales"))

    // Show processed data
    salesTotal.show()

    // Write the output to a Parquet file
    val outputDirectory = "/Users/vinodh/datasets/dataset.parquet"  // Update this path
    salesTotal.write.parquet(outputDirectory)

    // Stop the Spark session
    spark.stop()
  }
}

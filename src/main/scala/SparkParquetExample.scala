import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkParquetExample {
  def main(args: Array[String]): Unit = {
    // Initialize Spark Session
    val spark = SparkSession.builder
      .appName("Simple Parquet Example")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._

    // Create a DataFrame
    val data = Seq(
      ("James", 34, "New York"),
      ("Michael", 33, "California"),
      ("Robert", 37, "Las Vegas")
    )

    val df = data.toDF("Name", "Age", "City")

    // Write the DataFrame to a Parquet file
    df.write.mode("overwrite").parquet("/tmp/people.parquet")

    // Read the Parquet file
    val parquetFileDF = spark.read.parquet("/tmp/people.parquet")
    parquetFileDF.show()

    // Stop the Spark session
    spark.stop()
  }
}

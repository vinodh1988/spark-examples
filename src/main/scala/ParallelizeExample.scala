import org.apache.spark.sql.SparkSession

object ParallelizeExample {
  def main(args: Array[String]): Unit = {
    // Create a SparkSession. No need to create SparkContext
    // as it is created automatically when creating SparkSession.
    val spark = SparkSession.builder
      .appName("Parallelize Example")
      .config("spark.master", "local")
      .getOrCreate()

    // Get the SparkContext from SparkSession
    val sc = spark.sparkContext

    // Define a local Scala collection of numbers
    val data = Array(1, 2, 3, 4, 5)

    // Parallelize the collection to form an RDD
    val distData = sc.parallelize(data)

    // Perform a simple operation: summing up the values
    val sum = distData.reduce((a, b) => a + b)

    // Print the result
    println(s"The sum of the elements is $sum")

    // Stop the SparkSession
    spark.stop()
  }
}

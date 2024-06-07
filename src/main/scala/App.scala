import org.apache.spark.sql.SparkSession

object App {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Word Count")
      .master("local[*]") // Use local mode
      .getOrCreate()

    // Replace the path below with the path to your input text file
    val inputFilePath = "src/main/resources/data.txt"

    // Read the file into an RDD
    val textFile = spark.sparkContext.textFile(inputFilePath)

    // Split each line into words, count each word
    val counts = textFile.flatMap(line => line.split("\\s+"))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    // Collect and print the results
    counts.collect().foreach(println)

    spark.stop()
  }
}

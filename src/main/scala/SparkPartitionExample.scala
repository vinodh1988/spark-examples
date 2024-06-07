import org.apache.spark.sql.SparkSession

object SparkPartitionExample {
  def main(args: Array[String]): Unit = {
    // Initialize SparkSession
    val spark = SparkSession.builder()
      .appName("Partitioning Example")
      .master("local[*]") // Use all available cores on the local machine
      .getOrCreate()

    val sc = spark.sparkContext // Get the SparkContext from the SparkSession

    // Create an RDD from a collection without specifying the number of partitions
    val data = 1 to 1000
    val rdd = sc.parallelize(data) // No number of partitions specified
    println(s"Default number of partitions: ${rdd.partitions.size}")

    // Check the data distribution across partitions
    val partitionSizes = rdd.glom().map(_.length).collect()
    println(s"Data distribution before repartitioning: ${partitionSizes.mkString(", ")}")

    // Simple transformation and action to show the use of partitions
    val sumBeforeRepartition = rdd.reduce(_ + _)
    println(s"Sum before repartition: $sumBeforeRepartition")

    // Repartition the RDD into a higher number of partitions
    val repartitionedRDD = rdd.repartition(12)
    println(s"Number of partitions after repartition: ${repartitionedRDD.partitions.size}")

    // Check new data distribution across partitions
    val newPartitionSizes = repartitionedRDD.glom().map(_.length).collect()
    println(s"Data distribution after repartitioning: ${newPartitionSizes.mkString(", ")}")

    // Perform a reduction operation on the repartitioned RDD
    val sumAfterRepartition = repartitionedRDD.reduce(_ + _)
    println(s"Sum after repartition: $sumAfterRepartition")

    // Stop the Spark session
    spark.stop()
  }
}

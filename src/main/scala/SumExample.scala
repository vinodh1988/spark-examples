import org.apache.spark.sql.SparkSession

object SumExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Sum All Elements")
      .master("local[*]")
      .getOrCreate()

    val numbersRDD = spark.sparkContext.parallelize(Seq(1, 2, 3, 4, 5))
    val sum = numbersRDD.reduce(_ + _)

    println(s"Total Sum: $sum")
    spark.stop()
  }
}

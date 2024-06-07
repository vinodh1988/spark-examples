import org.apache.spark.sql.SparkSession

object MaxExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Find Max")
      .master("local[*]")
      .getOrCreate()

    val numbersRDD = spark.sparkContext.parallelize(Seq(10, 23, 3, 43, 5))
    val maxNumber = numbersRDD.reduce(Math.max(_, _))

    println(s"Maximum Number: $maxNumber")
    spark.stop()
  }
}

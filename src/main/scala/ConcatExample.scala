import org.apache.spark.sql.SparkSession

object ConcatExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Concat Strings")
      .master("local[*]")
      .getOrCreate()

    val wordsRDD = spark.sparkContext.parallelize(Seq("Apache", "Spark", "is", "awesome"))
    val sentence = wordsRDD.reduce(_ + " " + _)

    println(s"Sentence: $sentence")
    spark.stop()
  }
}

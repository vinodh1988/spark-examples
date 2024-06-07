import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkMySQLExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark MySQL Integration")
      .master("local[*]")
      .getOrCreate()

    // Define MySQL JDBC URL and table names
    val url = "XXXXXXXXXXXXXX"
    val dbtable = "people"
    val destinationTable = "result"
    val user = "sqladmin"
    val password = "XXXXXXXX"

    // Read data from MySQL database
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", url)
      .option("dbtable", dbtable)
      .option("user", user)
      .option("password", password)
      .load()

    jdbcDF.show()

    // Example transformation: filter operation
    val processedDF = jdbcDF.filter("city='Chennai'")

    // Write data back to MySQL in another table
    processedDF.write
      .format("jdbc")
      .option("url", url)
      .option("dbtable", destinationTable)
      .option("user", user)
      .option("password", password)
      .mode(SaveMode.Overwrite) // Overwrite existing data
      .save()

    spark.stop()
  }
}

import org.apache.spark.sql.SparkSession
/*
object S3Example {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("S3 Example with Spark")
      .master("local[*]")
      .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
      .getOrCreate()

    // Set up the configuration to access S3
    val hadoopConfig = spark.sparkContext.hadoopConfiguration
    hadoopConfig.set("fs.s3a.access.key", "XXXXxx")
    hadoopConfig.set("fs.s3a.secret.key", "XXXXXXXXXXXX")
    hadoopConfig.set("fs.s3a.endpoint", "s3.amazonaws.com")
    hadoopConfig.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    // Read data from S3
    val s3Data = spark.read.json("s3a://scalacourse803/people.json")

    // Process data
    val processedData = s3Data.groupBy("city").count()

    // Show processed data
    processedData.show()



    spark.stop()
  }
}
*/
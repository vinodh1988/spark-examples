import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataFrameExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataFrame Example")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Creating a DataFrame from a collection of data
    val data = Seq(("James", "Sales", 3000),
      ("Michael", "Sales", 4600),
      ("Robert", "IT", 4100),
      ("Maria", "Finance", 3000))
    val df = data.toDF("Employee Name", "Department", "Salary")

    // Show the DataFrame
    df.show()

    // Perform transformations
    val averageSalary = df.groupBy("Department").agg(avg("Salary").alias("Average Salary"))

      averageSalary.show()

    // Filtering
    val highEarners = df.filter($"Salary" > 4000)    //x => boolean expression
                                                     // $"Salary"
    highEarners.show()

    spark.stop()
  }
}

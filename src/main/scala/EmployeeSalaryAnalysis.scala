import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object EmployeeSalaryAnalysis {
  def main(args: Array[String]): Unit = {
    // Initialize SparkSession
    val spark = SparkSession.builder()
      .appName("Employee Salary Analysis")
      .master("local[*]")  // Use local mode
      .getOrCreate()

    // For implicit conversions from RDDs to DataFrames
    import spark.implicits._

    // Read data from a CSV file
    val filePath = "/Users/vinodh/datasets/employee_data.csv"  // Corrected dataset path
    val employeeDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(filePath)

    // Show the initial data
    employeeDF.show()

    // Process the data: Calculate average salary per department
    val averageSalaryByDept = employeeDF.groupBy("department")
      .agg(avg("salary").alias("average_salary"))

    // Show processed data
    averageSalaryByDept.show()

    // Write the output to another CSV file
    val outputDirectory = "/Users/vinodh/datasets/output_csv"  // Specify the output directory
    averageSalaryByDept.write
      .option("header", "true")
      .csv(outputDirectory)

    // Stop the Spark session
    spark.stop()
  }
}

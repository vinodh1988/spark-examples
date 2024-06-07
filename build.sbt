ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.3.3"

lazy val root = (project in file("."))
  .settings(
    name := "scala-spark"
  )

scalaVersion := "2.12.10" // Spark 3.2.x supports Scala 2.12.x

// Define Spark version that has better compatibility with newer Java versions
val sparkVersion = "3.2.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.hadoop" % "hadoop-common" % "3.2.0",
  "org.apache.hadoop" % "hadoop-aws" % "3.2.0",
  "com.amazonaws" % "aws-java-sdk-bundle" % "1.11.375",
  "mysql" % "mysql-connector-java" % "8.0.19"
)


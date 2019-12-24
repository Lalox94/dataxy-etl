import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.sql.{DataFrameReader, SparkSession}
import sys.process._

object postgresql {

    def main(args: Array[String]): Unit = {

      // Spark Configuration
      val conf = new SparkConf()
        .setMaster("local[2]")
        .setAppName("sql")
        .set("spark.executor.memory", "1g")

      // Get or Create a new Spark Context
      val sc = new SparkContext(conf)
      val spark = SparkSession
        .builder()
        .getOrCreate()

      val url = "jdbc:postgresql://localhost:5432/shoes"
      val connectionProperties = conecctionPropertiesConf()
      val query1 = "(SELECT * FROM shoes_walmart) as q1"
      val sparkDF = sparkReadDF(spark, query1, url, connectionProperties)

      println(sparkDF.show())
      sparkDF.write.format("com.databricks.spark.csv").option("header", "true").save("data")
      spark.close()
      sc.stop()

      val command = "mv data/*.csv data/walmart_shoes.csv"
      val shPath = "/bin/sh"
      val chnageNameCSV = Seq(shPath,  "-c", command).!
      if (chnageNameCSV.equals(0)) println("CSV Name has changed!!")
      else println("CSV name not Changed!!")

    }

  def conecctionPropertiesConf(): Properties ={
    val connectionProperties = new Properties()
    connectionProperties.setProperty("Driver", "org.postgresql.Driver")
    connectionProperties.setProperty("user", "dataxy")
    connectionProperties.setProperty("password", "dataxy")

    connectionProperties

  }

  def sparkReadDF(spark:SparkSession, query:String,url:String, connectionProperties: Properties):sql.DataFrame={
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/shoes")
      .option("dbtable", "public.shoes_walmart")
      .option("query", "SELECT * FROM public.shoes_walmart LIMIT 5")
      .option("user", "dataxy")
      .option("password", "dataxy")
      .load()

    val df = spark.read.jdbc(url, query, connectionProperties)

    df
  }
}

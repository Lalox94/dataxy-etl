import org.apache.spark.{SparkConf, SparkContext, sql}
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._

object cassandra {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("sql")
      .set("spark.executor.memory", "1g")

    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().getOrCreate()
    val cdbConnector = CassandraConnector(sc)
    val createKeyspace = "CREATE KEYSPACE IF NOT EXISTS walmart_shoes WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 } "
    cdbConnector.withSessionDo(session => session.execute(createKeyspace))

    val dfCSV = spark.read
      .format("org.databrick.spark.csv")
      .option("header", "true")
      .csv("data/walmart_shoes.csv")

    dfCSV.write
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> "shoes_walmart", "keyspace" -> "walmart_shoes"))
        .mode(org.apache.spark.sql.SaveMode.Append)
        .save()

    sc.stop()
  }

}

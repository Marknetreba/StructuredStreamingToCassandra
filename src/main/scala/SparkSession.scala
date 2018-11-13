import org.apache.spark.{SparkConf, sql}
import org.apache.spark.sql.SparkSession

class SparkSession extends Serializable {

  def buildSparkSession = {

    @transient lazy val conf: SparkConf = new SparkConf()
      .setAppName("StopBot")
      .set("spark.cassandra.connection.host", "localhost")
      //.set("spark.sql.streaming.checkpointLocation", "checkpoint")

    @transient lazy val spark = SparkSession
      .builder()
      .master("local[2]")
      .config(conf)
      .getOrCreate()

    spark
  }
}


object Structured extends SparkSession {
  def main(args: Array[String]): Unit = {

    val spark = buildSparkSession

    import spark.implicits._

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "127.0.0.1:9092")
      .option("subscribe", "data")
      .option("startingOffsets", "latest")
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
      .select("value")

    val query = df.writeStream
      .format("org.apache.spark.sql.cassandra")
      .outputMode("update")
      .foreach(new CassandraSink)
      .start()

    query.awaitTermination()
  }
}


object Structured extends SparkSession {

  def main(args: Array[String]): Unit = {

    val df = KafkaSource.initialize()

    val result = DFAnalyze.analyze(df)

    //Console output
    val cons = result.writeStream
      .format("console")
      .outputMode("complete")
      .start()

    cons.awaitTermination()


    //Cassandra output
    //    val cassandra = result.writeStream
    //      .format("org.apache.spark.sql.cassandra")
    //      .outputMode("update")
    //      .foreach(new CassandraSink)
    //      .start()
    //
    //    cassandra.awaitTermination()
  }
}

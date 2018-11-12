import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object Structured extends SparkSession {
  def main(args: Array[String]): Unit = {

    val spark = buildSparkSession

    import spark.implicits._

    val schema = new StructType()
      .add(StructField("unix_time", StringType, false))
      .add(StructField("category_id", StringType, false))
      .add(StructField("ip", StringType, false))
      .add(StructField("type", StringType, false))

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "127.0.0.1:9092")
      .option("subscribe", "data")
      .option("startingOffsets", "latest")
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
      .select(from_json(col("value"), schema) as "data")
      .select("data.unix_time","data.category_id","data.ip","data.type")
      .toDF("time","category","ip","type")

    df.printSchema()

    val cons = df.writeStream
      .format("console")
      .outputMode("append")
      .start()

    cons.awaitTermination()


//    val cassandra = df.writeStream
//      .format("org.apache.spark.sql.cassandra")
//      .outputMode("update")
//      .foreach(new CassandraSink)
//      .start()
//
//    cassandra.awaitTermination()
  }
}

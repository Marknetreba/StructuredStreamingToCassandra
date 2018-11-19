import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

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
      .na.drop()
      .select("ip","time","type", "category")
      .groupBy("ip")
      .agg(min(from_unixtime(col("time"),"yyyy-MM-dd HH:mm:ss")) as "time_start",
        max(from_unixtime(col("time"),"yyyy-MM-dd HH:mm:ss")) as "time_end",
        collect_set("category") as "categories",
        count(when(col("type")==="click", 1)) as "clicks",
        count(when(col("type")==="view", 1)) as "views",
        count("type") as "requests")

      .withColumn("distinct_categories",size(col("categories")))
      .withColumn("duration_minutes",from_unixtime(unix_timestamp(col("time_end")).minus(unix_timestamp(col("time_start"))),"mm"))
      .withColumn("event_rate",col("requests").divide(col("duration_minutes")))
      .withColumn("categories_rate",col("distinct_categories").divide(col("duration_minutes")))
      .withColumn("views_clicks", when(col("views")>0, col("clicks").divide(col("views"))).otherwise(col("clicks")))
      .withColumn("bot", when(col("views_clicks")>3 or col("categories_rate")>0.5 or col("event_rate")>100,"yes").otherwise("no"))
      .select("ip","bot","duration_minutes","distinct_categories","event_rate","categories_rate","views_clicks")

    //Console output
//    val cons = df.writeStream
//      .format("console")
//      .outputMode("complete")
//      .start()
//
//    cons.awaitTermination()


    //Cassandra output
    val cassandra = df.writeStream
      .format("org.apache.spark.sql.cassandra")
      .outputMode("update")
      .foreach(new CassandraSink)
      .start()

    cassandra.awaitTermination()
  }
}

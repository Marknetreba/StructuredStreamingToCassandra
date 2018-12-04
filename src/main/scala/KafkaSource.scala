import Structured.buildSparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object KafkaSource {

  def initialize(): DataFrame = {

    val spark = buildSparkSession

    import spark.implicits._

    val schema = new StructType()
      .add(StructField("unix_time", StringType, false))
      .add(StructField("category_id", StringType, false))
      .add(StructField("ip", StringType, false))
      .add(StructField("type", StringType, false))

    val source = spark
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

    source
  }

}

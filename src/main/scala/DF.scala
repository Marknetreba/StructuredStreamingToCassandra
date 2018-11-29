import Structured.buildSparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object DF {

  def analyze(data: Seq[(String, String, String, String)]): DataFrame = {

    val spark = buildSparkSession

    import spark.implicits._

    val df =data
      .toDF("time", "category", "ip", "type")
      .groupBy("ip")
      .agg(min(col("time")) as "time_start",
        max(col("time")) as "time_end",
        collect_set("category") as "categories",
        collect_list("type") as "types",
        count(when(col("type") === "click", 1)) as "clicks",
        count(when(col("type") === "view", 1)) as "views",
        count("type") as "requests")
      .withColumn("distinct_categories", size(col("categories")))
      .withColumn("duration_minutes", from_unixtime(unix_timestamp(col("time_end")).minus(unix_timestamp(col("time_start"))), "mm"))
      .withColumn("event_rate", col("requests").divide(col("duration_minutes")))
      .withColumn("categories_rate", col("distinct_categories").divide(col("duration_minutes")))
      .withColumn("views_clicks", when(col("views") > 0, col("clicks").divide(col("views"))).otherwise(col("clicks")))

      .withColumn("bot", when(col("views_clicks") > 3 or col("categories_rate") > 0.5 or col("event_rate") > 100, "yes").otherwise("no"))

      .select("ip", "bot", "duration_minutes", "distinct_categories", "event_rate", "categories_rate", "views_clicks")

    df

  }

}

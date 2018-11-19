import com.datastax.spark.connector.cql.CassandraConnector

class CassandraDriver extends SparkSession {

  val spark = buildSparkSession

  val connector = CassandraConnector(spark.sparkContext.getConf)

  val namespace = "test"
  val foreachTableSink = "dataframe"
}

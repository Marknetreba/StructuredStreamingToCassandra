import org.apache.spark.sql.ForeachWriter

class CassandraSink extends ForeachWriter[org.apache.spark.sql.Row] {

  val cassandraDriver = new CassandraDriver()

  def open(partitionId: Long, version: Long) = {
    println(s"Open connection")
    true
  }

  def process(record: org.apache.spark.sql.Row) = {
    println(s"Process new $record")
    cassandraDriver.connector.withSessionDo(session =>
      session.execute(
        s"""
       insert into ${cassandraDriver.namespace}.${cassandraDriver.foreachTableSink} (ip,bot,duration_minutes,distinct_categories,event_rate,categories_rate,views_clicks)
       values('${record(0)}','${record(1)}','${record(2)}','${record(3)}','${record(4)}','${record(5)}','${record(6)}')""")
    )
  }

  def close(errorOrNull: Throwable): Unit = {
    println(s"Close connection")
  }
}

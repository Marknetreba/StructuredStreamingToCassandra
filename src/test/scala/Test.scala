import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.functions._
import org.scalatest.FunSuite

class Test extends FunSuite with DataFrameSuiteBase {

  test("Categories rate more than 5 categories in 10") {


    val data = Seq(("2018-01-12 12:10:00", "1000", "172.10.0.0", "view"),
                    ("2018-01-12 12:15:00", "1001", "172.10.0.0", "view"),
                    ("2018-01-12 12:15:00", "1002", "172.10.0.0", "view"),
                    ("2018-01-12 12:15:00", "1003", "172.10.0.0", "click"),
                    ("2018-01-12 12:15:00", "1004", "172.10.0.0", "click"),

                    ("2018-01-12 12:10:00", "1000", "172.20.0.0", "view"),
                    ("2018-01-12 12:15:00", "1000", "172.20.0.0", "click"))

    val df = DF.analyze(data)

    println("CATEGORIES RATE > 5 categories/10 minutes")

    df.show()

    val bots = df.select(col("*")).where("bot == 'yes'")

    assert(bots.count(), 1)
  }

  test("Clicks/views more than 3") {

    val data = Seq(("2018-01-12 12:10:00", "1000", "172.10.0.0", "view"),
                    ("2018-01-12 12:10:00", "1000", "172.10.0.0", "click"),
                    ("2018-01-12 12:15:00", "1000", "172.10.0.0", "click"),
                    ("2018-01-12 12:15:00", "1001", "172.10.0.0", "click"),
                    ("2018-01-12 12:15:00", "1001", "172.10.0.0", "click"),

                    ("2018-01-12 12:10:00", "1000", "172.20.0.0", "view"),
                    ("2018-01-12 12:15:00", "1000", "172.20.0.0", "click"))

    val df = DF.analyze(data)

    println("CLICKS/VIEWS > 3")

    df.show()

    val bots = df.select(col("*")).where("bot == 'yes'")

    assert(bots.count(), 1)

  }

  test("Event rate more than 1000 requests in 10 minutes") {

    val events = for (i <- 1 to 500) yield ("2018-01-12 12:10:00", "1000", "172.10.0.0", "view")

    val data = Seq(("2018-01-12 12:15:00", "1000", "172.10.0.0", "click"),

                    ("2018-01-12 12:10:00", "1000", "172.20.0.0", "view"),
                    ("2018-01-12 12:15:00", "1000", "172.20.0.0", "click")).++(events)

    val df = DF.analyze(data)

    println("EVENT RATE > 100 events/minute")

    df.show()

    val bots = df.select(col("*")).where("bot == 'yes'")

    assert(bots.count(), 1)
  }

}

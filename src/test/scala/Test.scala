import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.functions._
import org.scalatest.FunSuite

class Test extends FunSuite with DataFrameSuiteBase {

  test("Categories rate more than 5 categories in 10") {

    // Unix timestamp conversion
    // 1543666200 ->  12/01/2018 12:10pm
    // 1543666500 ->  12/01/2018 12:15pm

    val data = Seq(("1543666200", "1000", "172.10.0.0", "view"),
                    ("1543666500", "1001", "172.10.0.0", "view"),
                    ("1543666500", "1002", "172.10.0.0", "view"),
                    ("1543666500", "1003", "172.10.0.0", "click"),
                    ("1543666500", "1004", "172.10.0.0", "click"),

                    ("1543666200", "1000", "172.20.0.0", "view"),
                    ("1543666500", "1000", "172.20.0.0", "click"))

    val df = DF.analyze(data)

    println("CATEGORIES RATE")

    df.show()

    val bots = df.select(col("*")).where("bot == 'yes'")

    assert(bots.count(), 1)
  }

  test("Clicks/views more than 3") {

    val data = Seq(("1543666200", "1000", "172.10.0.0", "view"),
                    ("1543666200", "1000", "172.10.0.0", "click"),
                    ("1543666500", "1000", "172.10.0.0", "click"),
                    ("1543666500", "1001", "172.10.0.0", "click"),
                    ("1543666500", "1001", "172.10.0.0", "click"),

                    ("1543666200", "1000", "172.20.0.0", "view"),
                    ("1543666500", "1000", "172.20.0.0", "click"))

    val df = DF.analyze(data)

    println("CLICKS/VIEWS")

    df.show()

    val bots = df.select(col("*")).where("bot == 'yes'")

    assert(bots.count(), 1)

  }

  test("Event rate more than 1000 requests in 10 minutes") {

    val events = for (i <- 1 to 500) yield ("1543666200", "1000", "172.10.0.0", "view")

    val data = Seq(("1543666500", "1000", "172.10.0.0", "click"),

                    ("1543666200", "1000", "172.20.0.0", "view"),
                    ("1543666500", "1000", "172.20.0.0", "click")).++(events)

    val df = DF.analyze(data)

    println("EVENT RATE")

    df.show()

    val bots = df.select(col("*")).where("bot == 'yes'")

    assert(bots.count(), 1)
  }

}

package sparkwordcount

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.mean
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.udf
import java.util.concurrent.TimeUnit
import java.util.Date
import java.text.SimpleDateFormat
import org.apache.spark.sql.functions.date_add
import org.apache.spark.sql.functions._
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import org.joda.time.DateTime

object DateFunctionWorking extends App {

  val spark = SparkSession.builder().appName("DateFunctionWorking").master("local").getOrCreate()

  //case class DaysExample(id: Int, date: String, days: Int)

  import spark.implicits._

  val custDF = spark.sparkContext.parallelize(Seq(
    (1, "2017-01-01", 10),
    (2, "2017-01-01", 20))).toDF("id", "current_date", "days")

  val newDF = custDF.withColumn("new_date", date_add($"current_date", 10))
  custDF.withColumn("dayofweek", date_format(to_date($"current_date", "MM/dd/yyyy"), "EEEE"))

  import java.util.Calendar
  import java.text.SimpleDateFormat

  def date_finding(x: String) = {
    val now = Calendar.getInstance.getTime
    val sdf = new SimpleDateFormat("yyyy-MM-dd").parse(x)
    val sdf1 = new SimpleDateFormat("yyyy-MM-dd")
    val d = new SimpleDateFormat("u").format(sdf)
    var c = Calendar.getInstance();
    c.setTime(sdf);
    var dayOfWeek = c.get(Calendar.DAY_OF_WEEK);
    var diff = Calendar.getInstance().getTime().getTime();

    val dayFormat = new SimpleDateFormat("dd")
    val dayofmonth = dayFormat.format(sdf).toInt
    val dateTime = new DateTime(sdf);

    val result = dayOfWeek match {
      case 2 => d
      case 1 =>
        {
          println("dayofmonth:::::::::::" + dayofmonth)
          if (sdf.getMonth == dateTime.minusDays(7 - dayOfWeek).toDate().getMonth) {
            dateTime.minusDays(7 - dayOfWeek).toDate();
          } else {
            c.set(Calendar.DATE, c.getActualMinimum(Calendar.DATE));
            sdf1.format(c.getTime)
          }

        }
      case _ =>
        {
          println("dayofmonth:::::::::::" + dayofmonth)
          if (sdf.getMonth == dateTime.minusDays(dayOfWeek - 2).toDate().getMonth) {
            println("dateTime.minusDays(dayOfWeek-2).toDate()::::::::::" + dateTime.minusDays(dayOfWeek - 2).toDate())
            sdf1.format(dateTime.minusDays(dayOfWeek - 2).toDate())

          } else {
            c.set(Calendar.DATE, c.getActualMinimum(Calendar.DATE));
            sdf1.format(c.getTime)
          }
        }
    }
    result
  }

  println(date_finding("2019-05-02"))

}
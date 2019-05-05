package sparkwordcount

import java.util.Calendar
import java.text.SimpleDateFormat
import scala.collection.mutable.ListBuffer

object Daysin24Months extends App {

  def FindingDateAndDay() = {
    val dowText = new SimpleDateFormat("E") // format of the day of the week
    val dateTxt = new SimpleDateFormat("MM/dd/yyyy") //format of the output date

    //This is used to save the start date
    var startD = Calendar.getInstance

    //This is used to save the end date
    var startDate = Calendar.getInstance
    startDate.add(Calendar.MONTH, 24)
    var endDate = startDate

    var day = new ListBuffer[String]()
    var date = new ListBuffer[String]()
    println("startDate :::::::::::" + startD.getTime)
    println("EndDate :::::::::::" + endDate.getTime)

    while (startD.getTime.before(endDate.getTime)) {
      day += (dowText.format(startD.getTime))
      startD.add(Calendar.MONTH, 1)
      date += (dateTxt.format(startD.getTime))

    }

    var result = date.zip(day).toList
    result
  }

  println(FindingDateAndDay)
}
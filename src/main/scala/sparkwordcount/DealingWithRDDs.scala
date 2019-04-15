package sparkwordcount

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object DealingWithRDDs extends App{
  
  //1. declare a spark conf
    val sc = SparkSession.builder()
    .master("local")
    .appName("SparkSessionZipsExample")
    .getOrCreate()
  
    
  
  val distFile = sc.sparkContext.textFile("sample.txt")
  var s = distFile.map(x => x.length()).reduce((a,b) => a+b)
  println(s)
  
  
  
  //wholeTextFiles takes the path and returns the file location and the content
  val dirFiles = sc.sparkContext.wholeTextFiles(s"D://data to VM")
//  println(dirFiles.collect().mkString(","))

//dealing with cache in RDD
  var cachedata = distFile.cache()
  var warnings = cachedata.filter(lines => lines.contains("WARN")).collect().toList
  var errors = cachedata.filter(lines => lines.contains("ERROR")).collect().toList
  
  println("Value with warnings count : %s \n Value with errors : %s".format(warnings,errors))
  
}



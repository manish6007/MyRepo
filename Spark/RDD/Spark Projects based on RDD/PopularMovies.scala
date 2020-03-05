package com.fis.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import java.util.Date
import java.text.SimpleDateFormat

object PopularMovies {
  
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using the local machine
    val sc = new SparkContext("local", "PopularMovies")   
    val data = sc.textFile("C:/Users/e1091444/Desktop/Spark/RDD/popular_movies.txt")
    val moviesdata = data.map(x=>(x.split("\t")(1),1)).reduceByKey(_+_).map(x=>(x._2,x._1)).sortByKey(false).top(10)
    println("Top 10 Popular movies:")
    for(result<-moviesdata){
      val movieid = result._2
      val count = result._1
      println(s"movieid:$movieid\nCount:$count\n")
       }
     // scala function to convert unix time in the data set in "yyyy-mm-dd" format
      def unix_to_date(line:String):String = {
        val unix_time = line.split("\t")(3).toInt
        val a = unix_time * 1000L
        val date_format = new SimpleDateFormat("yyyy-mm-dd")
        val result = date_format.format(a)
        return result       
        }
      
      val date = data.map(unix_to_date).take(10)
      date.foreach(println)
      
      //Average rating given by users for particular movie id
       
      def parseLine(line: String)={
        val splits = line.split("\t")
        val movieid = splits(1).toString()
        val rating = splits(2).toFloat
        (movieid,rating)  
      }
      
      val p = data.map(parseLine)
      val q = p.mapValues(x=>(x,1)).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
      val s = q.mapValues(x=>x._1/x._2).sortBy(x=>x._2,false).collect
      println("\nAverage rating given by users for particular movie id")
      s.foreach(println)
   
  }
}

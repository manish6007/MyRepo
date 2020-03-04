package com.fis.spark
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._


object WordCount {
   /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "WordCount")
   
    // Load up each line of the ratings data into an RDD
    val book = sc.textFile("../SparkScala/book.txt")
    
    val words = book.flatMap(x=>x.split("\\W+"))
    val lowercasewords = words.map(x=>x.toLowerCase()).map(x=>(x,1)).reduceByKey((x,y)=>(x+y))
    
    val wordCountFlip = lowercasewords.map(x=>(x._2,x._1))
    val WordCountSorted = wordCountFlip.sortByKey()
    
    for(result<-WordCountSorted){
      val word = result._2
      val count = result._1
      println(s"$word  $count")
    }
}
}
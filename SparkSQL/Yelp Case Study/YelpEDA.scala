package com.fis.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._


object YelpEDA {
   def main(args: Array[String]) {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("YelpEDA")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
      
     //Loading the json file and register the table in SPARK SQL
     val biz =spark.read.json("business.json")
     biz.printSchema()
     biz.createOrReplaceTempView ("biz")
     biz.cache()
     
     //Count the total number of records from the table
     spark.sql("select count(1) as businesses from biz").show()
     
     //Find state and count the number of businesses corresponding to that state.
     spark.sql("select state,count(1) as businesses from biz group by state").show()
     
     //Find state and count the number of businesses in descending order and restricting it to 1 record
     spark.sql("select state,count(1) as businesses from biz group by state order by businesses DESC limit 1").show()
     
     //Find name,stars,review_count,city and state whose star rating is 5.0 and state is PA
     spark.sql("select name,stars,review_count,city,state from biz where stars = 5.0 and state='PA'").show(3)
     
     //Find state and sum of review_count on the basis of state.
     spark.sql("select state,sum(review_count) as total_reviews from biz group by state").show()
     
     //Find state and average of review_count on the basis of state.
     spark.sql("select state,round(avg(review_count),2) as avg_reviews from biz group by state").show()
     
   }
}

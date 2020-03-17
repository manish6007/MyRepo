package com.fis.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._

object DataFrameQueries {
  def main(args: Array[String]) {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("DataFrameQueries")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
      
      val df = spark.read.parquet("C:/Users/e1091444/Desktop/Spark/SparkSQL/class_material26thmay/datasets/datasets/part-r-00001_gz.parquet")
      df.cache()
      df.printSchema()
      df.show()
      val firstNameDF = df.select("firstName", "year")
      println("firstname Dataframe:")
      firstNameDF.show()
      
      println("Count of firstname Dataframe:"+firstNameDF.count())
           
      println("Distinct count in firstname Dataframe:" + firstNameDF.select("firstName").distinct().count())
     
            
     /**Things to Note**
			1. Look closely, and you'll see a 'desc' after the 'orderBy()' call. 'orderBy()' (which can also be invoked as 'sort()') sorts in ascending order. Adding the 'desc' suffix causes the sort to be in _descending_ order.
			2. The Scala DataFrames API's comparison operator is '===' (_triple_ equals), not the usual '==' (_double_ equals). If you get it wrong, you'll get a Scala compiler error.*/
      
      //use the original data frame to find the five most popular names for girls born in 1980.
      (df.filter(df("year")===1880).
        filter(df("gender")==="F").
        orderBy(df("total").desc,df("firstName")).
        select("firstName").limit(5)).show()
        
        import spark.implicits._
       // We can also use the '$' interpolator syntax to produce column references to find the five most popular names for girls born in 1980:
     (df.filter($"year" === 1980).
           filter($"gender" === "F").
           orderBy($"total".desc, $"firstName").
           select("firstName").
           limit(5)).show 
           
      // How popular were the top 10 female names of 1890 back in 1880?
      //Before we can do that, though, we need to define a utility function. The DataFrame Scala API doesn't support the SQL 'LOWER' function. To ensure that our data matches up properly, it'd be nice to force the names to lower case before doing the match. Fortunately, it's easy to define our own 'LOWER' function:
        val lower = spark.udf.register("lower", (s: String) => s.toLowerCase)
        
      val ssn1890 = df.filter($"year"===1890).
                    select($"total".as("total1890"),
                           $"gender".as("gender1890"),
                           lower($"firstName").as("name1890"))
                           
       val ssn1880 = df.filter($"year"===1880).
                     select($"total".as("total1880"),
                         $"gender".as("gender1880"),
                          lower($"firstName").as("name1880"))
                          
       val joined = ssn1890.join(ssn1880, ($"name1890" === $"name1880") && ($"gender1890" === $"gender1880")).
                     filter($"gender1890" === "F").
                     orderBy($"total1890".desc).
                     limit(10).
                     select($"name1890".as("name"), $"total1880", $"total1890")
       
         joined.show()
                           
  }
  
}

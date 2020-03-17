package com.fis.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._


object SanFranciscoFireDeptCallEDA {
  
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
      
    val input_df =spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", ",").load("fire.csv")
    input_df.printSchema()
    input_df.show(5)
    println("Columns in data are as below:")
    input_df.columns.foreach(println)
    println("\nNumber of records in file are:"+input_df.count())
    
    //Different types of calls were made to the Fire Department
    println("\nDifferent types of calls were made to the Fire Department:")
    input_df.select("Call Type").distinct().show(false)
    
    //Number of calls per call type
    println("\nNumber of calls per call type:")
    input_df.select("Call Type").groupBy("Call Type").count().show(false)
    
    //Number of partitions
    println("\nNumber of partitions: " + input_df.rdd.getNumPartitions)
    
    val cnt_partition = input_df.rdd.getNumPartitions
    // Limiting the partitions to 6 if number of partitions are greater than 6
    if (cnt_partition>6){
       val input_df_rep = input_df.repartition(6)
       println("\nNumber of partitions after repartition: " + input_df_rep.rdd.getNumPartitions)
    }
    
    input_df.createOrReplaceTempView("fire")
    
    // Print the schema of the table or view
    println("\nSchema of the table or view")
    spark.sql("DESC fire").show(50,false);
    
    //Number of rows in the table
    println("\nNumber of rows in the table")
    spark.sql("select count(1) from fire").show()
    
    //Distinct city  name in table
    println("\nDistinct city  name in table")
    spark.sql("select distinct city from fire").show()
    
    //City names in lexicographical order
    println("\nCity names in lexicographical order")
    spark.sql("select distinct city from fire order by city ").show()
    
    //Distinct Priorities  name in table
    println("\nDistinct Priorities in table")
    spark.sql("select distinct `Priority` from fire").show()
    
    //Neighborhood which generated the most number of calls
    println("\nNeighborhood which generated the most number of calls")
    spark.sql("select `Neighborhooods - Analysis Boundaries`,count(1) as number_of_calls from fire group by `Neighborhooods - Analysis Boundaries` order by number_of_calls").show(1)
      
  } 
}

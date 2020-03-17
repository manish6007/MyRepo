package com.fis.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._


object UserDataExploration {

    def main(args: Array[String]) {
    
      // Set the log level to only print errors
      Logger.getLogger("org").setLevel(Level.ERROR)
      
      val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
      
      import spark.implicits._
      
      val data = spark.sparkContext.textFile("../../Datasets/userdata.txt", 3).map(line=> line.split(',')).map(x=>(x(0),x(1),x(2),x(3),x(4)))
      //data.collect().foreach(println)
      val uniqueProfessions = data.map{case(id, age, gender, profession,zipcode) =>profession}.distinct().count()
      println("")
      println(s"\nNo. of uniqie professions $uniqueProfessions\n")
      
      val usersByProfession = data.map{case(id, age, gender, profession,zipcode) =>(profession,1)}
      println("")
      println(s"\nNo. of people in each professions:")
      usersByProfession.reduceByKey(_+_).sortBy(_._2,false).top(5).foreach(println)
      
      val usersByZipcode = data.map{case(id, age, gender, profession,zipcode) =>(zipcode,1)}
      println("")
      println(s"No. of people in each Zipcode:") 
      usersByZipcode.reduceByKey(_+_).sortBy(_._2,false).top(5).foreach(println)
      
      val usersByGender = data.map{case(id, age, gender, profession,zipcode) =>(gender,1)}
      println("")
      println(s"No. of people by Gender:") 
      usersByGender.reduceByKey(_+_).foreach(println)
}

}
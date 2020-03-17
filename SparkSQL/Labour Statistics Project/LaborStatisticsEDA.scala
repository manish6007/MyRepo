package com.fis.spark

import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark._

case class Income(Occupation:String,All_workers:Int,All_weekly:Int,M_workers:Int,M_weekly:Int,F_workers:Int,F_weekly:Int)
object LaborStatisticsEDA {
  
   def main(args: Array[String]) {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("LaborStatisticsEDA")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
      
      val a = spark.read.textFile("C:/Users/e1091444/Desktop/Spark/SparkSQL/class_material26thmay/datasets/datasets/inc_occ_gender3.csv")
      val header = a.first
      val data1 = a.filter( _ != header)
      val row1 = data1.first
      val data = data1.filter(_ != row1)

      
      import spark.implicits._
      val income_ds = data.map(_.split(",")).map(x => Income(x(0),x(1).toInt,x(2).toInt,x(3).toInt,x(4).toInt,x(5).toInt,x(6).toInt))
      income_ds.printSchema
      income_ds.show()
      income_ds.createOrReplaceTempView("salaries")
      
  
      //List out top 10 occupations with highest weekly income
      println("\nTop 10 occupations with highest weekly income")
      spark.sql("select occupation,all_weekly from salaries order by all_weekly desc").show(10)
      
      //List out top 10 occupations where female staff are more in number
      println("\nTop 10 occupations where female staff are more in number")
      spark.sql("select occupation,f_workers,m_workers from salaries where f_workers > m_workers order by f_workers desc").show(10,false)
      
      //List out top 10 occupations with more female income
      println("\nTop 10 occupations with more female income")
      spark.sql("select occupation,f_weekly from salaries where f_weekly > m_weekly order by f_weekly desc").show(10,false)
      
      //Income gap between males and females in America
      println("\nIncome gap between males and females in America")
      spark.sql("select sum(f_weekly)-sum(m_weekly) as income_gap_m_Vs_f from salaries").show()

   }  
}

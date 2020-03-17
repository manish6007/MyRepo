package com.fis.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
    
object DataFrames {
  
  case class Person(ID:Int, name:String, age:Int, numFriends:Int)
  case class YahooStockPrice(date:String, open:Float, high:Float, low:Float, close:Float, volume:Integer,adjClose:Float)
  def mapper(line:String): Person = {
    val fields = line.split(',')  
    
    val person:Person = Person(fields(0).toInt, fields(1), fields(2).toInt, fields(3).toInt)
    return person
  }
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
    
    // Convert our csv file to a DataSet, using our Person case
    // class to infer the schema.
    import spark.implicits._
    //Creating DataFrame from List
    
    //Example 1:
     val list = spark.sparkContext.parallelize(1 to 10)
     val df1 = list.map(i => (i,i*2)).toDF("single","double")
     df1.show()
     
     //Example 2
     val a = List(("ironman",3),("kabali",2),("bhahubali",5))
     val rating = a.toDF("movie","rating")
     rating.printSchema
     rating.show
     rating.createOrReplaceTempView("ratings")
     spark.sql("select * from ratings where rating > 4").show
     
     
     //Example 3
     import org.apache.spark.sql.DataFrame
     val df = Seq(("Yoda","Obi-Wan Kenobi"),("Anakin Skywalker", "Sheev Palpatine"),("Luke Skywalker","Han Solo, Leia Skywalker"),("Leia Skywalker","Obi-Wan Kenobi"),("Sheev Palpatine",  "Anakin Skywalker"),("Han Solo","Leia Skywalker, Luke kywalker, Obi-Wan Kenobi, Chewbacca"),("Obi-Wan Kenobi","Yoda, Qui-Gon Jinn"),("R2-D2","C-3PO"),("C-3PO","R2-D2"),("Darth Maul","Sheev Palpatine"),("Chewbacca","Han Solo"),("Lando Calrissian", "Han Solo"), ("Jabba","Boba Fett")).toDF("name", "friends")
     df.printSchema
     df.show()
    
     //creating dataframe using case class
     //Example 4
     
     val yahoo_stocks=spark.sparkContext.textFile("../Datasets/yahoo_stocks.csv")
     val header =yahoo_stocks.first
     val data =yahoo_stocks.filter(_ != header)
     val stockprice_temp=data.map(_.split(",")).map(row =>YahooStockPrice(row(0).trim.toString, row(1).trim.toFloat, row(2).trim.toFloat, row(3).trim.toFloat, row(4).trim.toFloat, row(5).trim.toInt, row(6).trim.toFloat))
     val stockprice = stockprice_temp.toDF
     println("DataFrame using case class")
     stockprice.printSchema
     stockprice.show()
     stockprice.createOrReplaceTempView("yahoo_stocks_temp") 
     spark.sql("select * from yahoo_stocks_temp").show()
     
     
     //create dataframe using struct
     //Example 5
     import org.apache.spark.sql.types._
     import org.apache.spark.sql.{Row, SparkSession}
     
     val schema = StructType(Array(StructField("name",StringType,true),StructField("age",IntegerType,true)))
     val rdd = spark.sparkContext.parallelize( Seq("john","Adom","Smith")).map(x => (x,20+x.length))
     val rowRDD = rdd.map(x => Row(x._1,x._2))	
     val df5 = spark.createDataFrame(rowRDD,schema)
     df5.createOrReplaceTempView("people")
     println("Struct Type")
     spark.sql("select * from people").show
     
     //Reading CSV data using databricks csv packages
     println("CSV with Databrics package")
     val characters_df = spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", ",").load("../Datasets/StarWars.csv")
     characters_df.show
     println("Record Count: " +characters_df.count())
     
     //Dealing with null and empty values
     import org.apache.spark.sql.functions._
     val filterdata = characters_df.filter(characters_df("haircolor") !== "")
     val changedata = characters_df.filter(characters_df("haircolor") === null || characters_df("haircolor") === "").withColumn("haircolor",lit("green"))
     println("Record Count after missing value removal:" +filterdata.unionAll(changedata).count())
     filterdata.unionAll(changedata).show
     
     // reading csv data without databricks csv packages
     println("CSV without Databrics package")
     val starwarcsv = spark.read.format("csv").option("inferSchema", "true").option("header","true").option("delimiter",",")load("../Datasets/StarWars.csv")
     starwarcsv.printSchema()
     starwarcsv.show()    
     
     //Reading JSON Files
     //Example 1 simple JSON
     println("JSON")
     val persons = spark.read.json("../Datasets/persons.json")
     persons.createOrReplaceTempView("persons")
     spark.sql("select * from persons").show
     
    
    //Example 2 Nested JSON
     println(" Nested JSON") 
     val employees_df = spark.read.json("../Datasets/employee.json")
     employees_df.createOrReplaceTempView("employees")
     spark.sql("select id,name,salary,address.city,address.country from employees").show
     
     
     //Dealing XML files
     println(" XML") 
     val employees_df_xml = spark.read.format("com.databricks.spark.xml").option("inferSchema", "true").option("rootTag","employees").option("rowTag","employee").load("../Datasets/employees.xml")
     val emp_dataNormal = employees_df_xml.select("emp_no","emp_name","address.city","address.country","address.pincode","salary","dept_no").show
     
     
    //Dealing PARQUET files
     println("PARQUET") 
     val baby_names_df = spark.read.parquet("../Datasets/baby_names.parquet")
     baby_names_df.show
     
     
         
    spark.stop()
  }
}

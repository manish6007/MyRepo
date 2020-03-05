Steps to install SBT
--------------------

To package a jar in Windows, firstly you have to install SBT. Follow the instructions below to install SBT

1. Download SBT .msi file from the link below

http://www.scala-sbt.org/download.html
 
2. Click on the downloaded .msi file and install it

3. To check whether SBT has been installed successfully or not, type below command in the prompt
 
sbt about


Problem statement: We will count the no of number of lines with "a" and "b" in the file stored in HDFS.

Create a directory structure in any one of the location


mkdir spark_testing
cd spark_testing/

create a sbt file with the command :   touch simple.sbt

src/main/scala
mkdir src
cd src/

mkdir main
cd main/

mkdir scala
cd scala/

vi SimpleApp.scala

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SimpleApp {
  def main(args: Array[String]) {
    val logFile = "YOUR_SPARK_HOME/README.md" // file stored on your HDFS
    val conf = new SparkConf().setAppName("Simple Application")
	val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    
  }
}


cd.. cd.. cd.. 


Maven coordinates:
-------------------
Jenkins is used to create the build for production in industry.	

vi simple.sbt
name := "Simple Project"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0"              // specify ur scala and spark versions in this sbt file


Package your application
------------------------
 
 Open a terminal and navigate to application root directory
 
 Type sbt package to create the JAR(This may take a while)
 
 Once finished, there will be a target directory. 
 
 Navigate to target--------->scala--------->
 
 You can find the JAR file here


Command to submit the JAR to Spark environment
-----------------------------------------------

spark-submit \
  --class "SimpleApp" \
  --master local[4] \
  target/scala-2.11/simple-project_2.11-1.0.jar
 
 
Running the JAR in local mode
 
 spark-submit hello-spark.jar --class HelloSpark --master local
 
Running the JAR in YARN client mode
 
 spark-submit hello-spark.jar --class HelloSpark --master yarn --deploy-mode client
  
Running the jar in YARN cluster mode
  
 spark-submit hello-spark.jar --class HelloSpark --master yarn --deploy-mode cluster


Maven coordinates:
-------------------
Jenkins is used to create the build for production in industry.	


  

 
 


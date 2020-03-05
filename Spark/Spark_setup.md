Spark
==========================
History of Spark
In 2011 at the University of Berkley, there was a project called AMPLab. This projcet was trying to create a new cluster resource manager. Later it came to be known as Mesos. In order to test the power of Mesos, a new framework called Spark was created.
Later Mesos became an Apache project
But, people thought Spark is really awesome!!!! and they started developing the source code further. Spark became an Apache project.
The people who created Spark, founded a company called Databricks.
Spark is quite new!!!!!!!

There are 4 reasons why Spark is popular
1. General Unified Engine.
2. It is super fast 
3. Ease of programming.
4. Portable

Mapreduce cannot do iterative processing.
Spark is fast due to in-memory computing

RDD is a pointer to the data.

Installing Spark on Windows
===========================
http://spark.apache.org/downloads.html

Choose Spark 2.3.0

Download winutils.exe 64 bit from link below

https://codeload.github.com/gvreddy1210/64bit/zip/master

Keep the winutils.exe file in  C:\hadoop\bin

Now, update your environment variables

Edit System variables  and add 

HADOOP_HOME = C:\hadoop

SPARK_HOME = C:\Spark

Edit path and add 

%HADOOP_HOME%\bin

%SPARK_HOME%\bin

Start spark using spark-shell

You can interact with Spark in two ways
---------------------------------------
1. Using the Shell
  Scala shell, Python shell AND R shell
2. Using an application JAR file

Once you have a spark program it can run either in local mode or cluster mode.
If you have a spark program, it has two components.
Master                            Slave
Driver                            Executor

Check the version of Spark

spark-shell --version

In order to start the scala shell of Spark

spark-shell

By default when you start the spark shell, by typing spark-shell, it will start in local mode. In local mode, bot the driver and executor runs inside a single JVM.

Check the Spark UI at http://sparkui.upxlabs.com:4040/jobs/

When you launch spark-shell it creates an Object called Spark Context (sc)





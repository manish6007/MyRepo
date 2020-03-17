Spark:
----------
Map Reduce- Processing framework on top of HDFS. It neither have storage nor the resource manager. Resources are managed by YARN. 

Spark is the classic replacement of map reduce. Spark is programming framework/Execution engine for like map reduce. Faster, Easy to use than map reduce. HDFS - Storage, YARN - Resorce Manager, Spark- Progamming framework.

Spark is plateform independent. It is not necessary to be installed on top of hadoop.

Drawbacks of mapreduce:
-------------------------------
1. Intermediate read and write.
2. Cannot perform interative processing.

Unique features of Spark:
1. Really fast: In memory computing(No intermediate read and write like map reduce.
2. Easy to program.
3. Unified engine(Can do sql batch processing, ML, Graph processing using single framework)
4. Can iterate multiple times in memory like for ML.
5. Huge list of connectivity options.

There are 5 API's in spark
1) Spark Core
2) Spark SQL
3) MLlib
4) Spark streaming
5) Graphx

At the end everything will be converted into Spark Core(RDD)

RDD is basic data structure in Spark.

Spark Architecture:
------------------------
1. Spark application runs as independent processes on a cluster.

2. Every spark application consists od a driver program that launches parallel operations on spark cluster.

3. Driver program access Spark through a SparkContext object. DP will not contain business logic. It contains only the settings to run on cluster.

Worker node is the data node. when the spark application is launched driver program intiated and it will ask for executor. Executor is nothing but the container(processing environment)

Driver will talk to yarn and ask for executor or containers. Once the executor is available the driver program will push the business logic(task) to executor for processing

Client will submit driver program. YARN will read the driver progranm through spark context and allocate the executor on worker nodes and then the task will be executed on worker nodes in parallel.

spark-shell is a driver. Spark-shell will start spark in local mode. In local mode there is no YARN. Spark will run on single machine. In local mode spark will launch driver and executor in a single container(JVM) but data can be read and write on hadoop.

spark-shell --master yarn will open spark in map reduce mode.

spark-shell --help

default driver memory =1GB
default executor memory =1GB

sc.getConf.getAll.foreach(println)

If we given 1G executor memory then we get only
(1024-300)*0.75=543 MB will be used by spark.

This will apply for driver memory also.

300MB is reserved 

75% can be used by spark 
25% will be used as user memory.

Data is divided in partions and executed by those many executors.

Always ask fore more executors in place of 1 xecutor with higher memory so take benefit of parallel processing


1 GB file in hadoop.

number of blocks=1024/128=8 = no. of partions can be 8

if we have 4 executors:

ex1 block 1,2

ex2 block 3,4

ex3 block 5,6

ex4 block 7,8

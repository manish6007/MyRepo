RDD-Resilient, Distributed, Dataset
1. Lowest level of abstraction in spark.
2. End of the day everything is RDD in spark.
3. RDD is the collection of elements(Grouping of data)
4. RDD's are by default immutable.

spark session object is used while working with spark sql.
Creating an RDD.
--------------------
1. From existing collection (Array, List, Sequence)
Eg. Val rdd = sc.parallelize(List("Manish","Rahul")
val a = sc.parallelize(1 to 100)

Transformations(Lazy) and Actions:
----------------------------------
Partitions in RDD: whenever we create an RDD the data is partitioned.

To identify number of partitions:
val a = sc. parallelize(1 to 100)
a.partition.length

Partition logic:
no. of partitions = no. of processor cores

we can change number of partitions by explicitely defining the partitions while creating the RDD.

val a = sc.parallelize(1 to 100,4)
4 = number of partitions

while reading from hdfs the number of partitions will be equal to number of blocks.

Collect is most used action when we use actions we can see the stats on Spark UI.

actions:
--------
collect
count
first
take
saveAsTextFile("<>")

Local Mode: Spark will run on a single machine.

To read from local/Linux:
--------------------------
val a = sc.textFile"file:///home/support1161/abc.txt")


Transformations:
--------------------
map
flatmap : flatten the output(In case of nested structure we need to use that)

val c = b.reduceByKey((x,y)=>(x +y))

Logic:
------
b = (Manish,1)(Manish,1)(Manish,1)

reduceByKey = (Manish,(1,1,1))
((x,y)=(x+y))

take any 2 values x and y and keep on adding them until all the values are over

                       x y
reduceByKey =(Manish,(1,1,1))

          x y
(Manish,(2,1))

Result: (Manish,3)

Note:
Two actions cannot be combined together but the transformations can be combined.

Why my program is running slow.

sortByKey(false)--> will sort data based on key in descending order for ascending order pass the argument as true.

sortBy(tuple)--> it can sort on either key or value. e.g. sortBy(_._2,false)

first _ represents whole tuple
second _ represents element within the tuple.

Job vs Stage vs Task:
-------------------------

All the transformations can be classified into 2 categories.

1. Narrow Transformations: They can be executed independently.(No dependency b/w the data). they operate within the partition.
e.g.:Map, Flatmap,Filter

2. Wide Transformations: If suffle is required b/w the partitions then they are called wide transformation.
e.g. reduceByKey,groupByKey,join etc.

Any transformation is called the task.
Stage is the collection of tasks.

All the series of operations before collect is called job.

whenever there is wide transformation spark will call for stage

------------------------------------
partitions are loaded into executors.
1 executor can process more than 1 partitions. tasks will be applied to partitions
-------------------------------------

Repartition:
------------
Transformation used to increase the number of patitions.

val numbers = sc.parallelize(1 to 100,2)

numbers.partitions.length
numbers.foreachPartitions(x=>println(x.toList))

val numberIncreased = numbers.reparition(10)

numbers.foreachPartitions(x=>println(x.toList))

If your code ends up doing shuffle the maximum amount of data that can be in a partition is 2 GB.

If the data is exeeding more than 2 GB in a partition then we will get the error java,lang. IllegalArgumentException: Size exceeds Integer.MAX_VALUE. In that case we need to repartition the data.

coalesce:
------------
Transformation used to decrease the number of partitions.

numberIncreased.partitions.length
10

val x =numberIncreased.coalesce(3)

use case:
-----------
we have a file having 1 billion rawa in S3. if we are reading full file we will have 1800 partitions.

val a = sc.textFile("bigFile.csv")==>1800 partition

val b = a.sample(false,.0001,34)==>1800 partitions

b contains 1000 lines , 1800 partitions
in this case we need to use coalesce to reduce number of paritions


why program is running slow:
-----------------------------------
skewness in the data:

when we represnt the data in key value pair some key will have more data and other will have less data. This is called skewness.

a, 1million--> 1 processor thread
q, 5000 -------> 1thread

To resolve this we use salting the key.
In this technique the key which is having more data will be divided.


Note: In order to increse or decrease the partition we can use reparition. Coalease decreases the number of shuffle operations.


Caching the RDD:
----------------------
When we want to run different different actions on RDD then it has to do all the process everytime we call an action because pipeline is getting emptyied after every action. To avoid such cases we will cache the last RDD and will use the same for different actions.

rdd.cache()

In storage section in UI we can see the cached RDD. By default it is caching the data in the RAM. If we don't have enough RAM then we have 5 different options using persist.

MEMORY_ONLY
MEMORY_ONLY_SER-->Compress and store
MEMORY_AND_DISK
MEMORY_AND_DISK_SER
DISK_ONLY

spark persist storagelevel.
for configuring above we need to import below library first.

import org.apache.spark.storage.StorageLevel._

book.persist(DISK_ONLY)

book.unpersist()-->to remove caching

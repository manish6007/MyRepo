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

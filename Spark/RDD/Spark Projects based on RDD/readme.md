Best Practices:
------------------
Keep 4-6 processor cores per executor.

6 machines with 64 GB RAM/machine and 16 processor cores per machine.

16*6 = 96 processor cores

64*6 = 384 GB RAM(Total)

96/4 = 24 executors each with 4 processor cores and 384/24 = 16 GB RAM per executor will be ideal cluster

In reality we will get around 10GB RAM and 4 cores.

You can very well load 10 partitions per executor.


/** Compute the average number of friends by age in a social network. */
object FriendsByAge {

/** A function that splits a line of input into (age, numFriends) tuples. */
def parseLine(line: String) = {
// Split by commas
val fields = line.split(",")
// Extract the age and numFriends fields, and convert to integers
val age = fields(2).toInt
val numFriends = fields(3).toInt
// Create a tuple that is our result.
(age, numFriends)
}

// Load each line of the source data into an RDD
val lines = sc.textFile("fakefriends.csv")

// Use our parseLines function to convert to (age, numFriends) tuples
val rdd = lines.map(parseLine)

rdd.collect K V K V K V
res14: Array[(Int, Int)] = Array((33,385), (26,2), (55,221), (40,465), (68,21), (59,318), (37,220), (54,307), (38,380), (27,181), (53,191), (57,372), (54,253), (56,444), (43,49), (36,49), (22,323), (35,13), (45,455), (60,246), (67,220), (19,268), (30,72), (51,271), (25,1), (21,445), (22,100), (42,363), (49,476), (48,364), (50,175), (39,161), (26,281), (53,197), (43,249), (27,305), (32,81), (58,21), (64,65), (31,192), (52,413), (67,167), (54,75), (58,345), (35,244), (52,77), (25,96), (24,49), (20,1), (40,254), (51,283), (36,212), (19,269), (62,31), (19,5), (41,278), (44,194), (57,294), (59,158), (59,284), (20,100), (62,442), (69,9), (58,54), (31,15), (52,169), (21,477), (48,135), (33,74), (30,204), (52,393), (45,184), (22,179), (20,384), (65,208), (40,459), (62,201), (40,407), (61,337), ...


// Lots going on here...
// We are starting with an RDD of form (age, numFriends) where age is the KEY and numFriends is the VALUE
// We use mapValues to convert each numFriends value to a tuple of (numFriends, 1)
// Then we use reduceByKey to sum up the total numFriends and total instances for each age, by
// adding together all the numFriends values and 1's respectively.
val totalsByAge = rdd.mapValues(x => (x, 1)).reduceByKey( (x,y) => (x._1 + y._1, x._2 + y._2))

rdd.mapValues(x => (x,1)).collect

res15: Array[(Int, (Int, Int))] = Array((33,(385,1)), (26,(2,1)), (55,(221,1)), (40,(465,1)), (68,(21,1)), (59,(318,1)), (37,(220,1)), (54,(307,1)), (38,(380,1)), (27,(181,1)), (53,(191,1)), (57,(372,1)), (54,(253,1)), (56,(444,1)), (43,(49,1)), (36,(49,1)), (22,(323,1)), (35,(13,1)), (45,(455,1)), (60,(246,1)), (67,(220,1)), (19,(268,1)), (30,(72,1)), (51,(271,1)), (25,(1,1)), (21,(445,1)), (22,(100,1)), (42,(363,1)), (49,(476,1)), (48,(364,1)), (50,(175,1)), (39,(161,1)), (26,(281,1)), (53,(197,1)), (43,(249,1)), (27,(305,1)), (32,(81,1)), (58,(21,1)), (64,(65,1)), (31,(192,1)), (52,(413,1)), (67,(167,1)), (54,(75,1)), (58,(345,1)), (35,(244,1)), (52,(77,1)), (25,(96,1)), (24,(49,1)), (20,(1,1)), (40,(254,1)), (51,(283,1)), (36,(212,1)), (19,(269,1)), (62,(31,1)), (19,(5,1)), (41,(278...
K V
(54,(307,1))
K V
(54,(75,1))
K V
(54,(253,1))

reduceByKey( (x,y) => (x._1 + y._1, x._2 + y._2))

(54, ((307,1), (75,1), (253,1) )

First Iteration
x y
(54, ((307,1), (75,1), (253,1) )
307 75 1 1
(x,y) => (x._1 + y._1, x._2 + y._2)

(54, (382,2), (253,1))

Second iteration
x y
(54, (382,2), (253,1))
382 253 1 1
(x,y) => (x._1 + y._1, x._2 + y._2)

(635,3)


SBT
------
How do you submit your program to cluster
------------------------------------------
1. Make sure you have scala installed in your PC
2. Download and install SBT in your PC
3. Verify the installation by running sbt about	

Maven coordinates:
-------------------

Jenkins is used to create the build for production in industry.

How to run the job:
-------------------
spark-submit hello-sparkapp_2.11-1.0.jar --class HelloSpark ------> This will run the program in local mode

spark-submit hello-sparkapp_2.11-1.0.jar --class HelloSpark --master yarn--> This will run the program in yarn mode



Broadcast variable:
-----------------------
Varible whose value will be available across the cluster(each and every node).

Broadcast objects to the executors, such that they're always there whenever needed.

just use sc.braodcast to shipoff whatever you want.

the use .value() to get the object back.


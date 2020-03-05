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
2. It is super fast!!!!!!!!!
3. Ease of programming.
4. Portable
Mapreduce cannot do iterative processing.
Spark is fast due to in-memory computing

Apache Pig                                              Spark

A = LOAD 'abc.txt';                                    A = read a file======>Transformations
B = FOREACH A, GENERATE something;                     B = filter A=========>Transformations
dump B                                                 collect B============>Action

It creates a DAG(Directed Acyclic Graph)

In Pig, A and B are called relations.

In Spark, A and B are called RDDs (Resilient Distributed Dataset)

RDD is a pointer to the data.

Installing Spark on Windows
===========================
http://spark.apache.org/downloads.html

Choose Spark 2.3.0

Download winutils.exe 64 bit from link below

https://codeload.github.com/gvreddy1210/64bit/zip/master

Keep the winutils.exe file in C:\hadoop\bin

Now, update your environment variables

Edit System variables  and add HADOOP_HOME = C:\hadoop

Edit path and add C:\hadoop\bin

<Locaion of your spark binaries>/bin

Start spark using spark-shell

You can interact with Spark in two ways
---------------------------------------
1. Using the Shell
Scala shell AND Python shell AND R shell
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

How to create an RDD
--------------------
1. Create an RDD from an existing collection
scala> val abc = List("Raghu", "Spark", "James Bond", "Hadoop")
abc: List[String] = List(Raghu, Spark, James Bond, Hadoop)
scala> val abc_rdd = sc.parallelize(abc)
abc_rdd: org.apache.spark.rdd.RDD[String] = ParallelCollectionRDD[0] at parallelize at <console>:29

scala> val b = Array(1,2,3,4,5,6)
b: Array[Int] = Array(1, 2, 3, 4, 5, 6)
scala> val c = sc.parallelize(b)
c: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[1] at parallelize at <console>:29

val xyz = sc.parallelize(1 to 100)

In order to create an RDD from a collection, use parallelize mathod.

If you have a 1 GB file in Hadoop, and block size is 128 MB, ity will have 8 blocks

If you create an RDD from this file in Hadoop, the RDD will by default have 8 partitions

All you need to do is ask for 8 executors

Create an RDD from an external file
------------------------------------
In order to create an RDD from an external file, use textFile method.

scala> val orders =sc.textFile("datasets/orders")
orders: org.apache.spark.rdd.RDD[String] = datasets/orders MapPartitionsRDD[1] at textFile at <console>:27

scala> orders.take(10)
res0: Array[String] = Array(1,2013-07-25 00:00:00.0,11599,CLOSED, 2,2013-07-25 00:00:00.0,256,PENDING_PAYMENT, 3,2013-07-25 00:00:00.0,12111,COMPLETE, 4,2013-07-25 00:00:00.0,8827,CLOSED, 5,2013-07-25 00:00:00.0,11318,COMPLETE, 6,2013-07-25 00:00:00.0,7130,COMPLETE, 7,2013-07-25 00:00:00.0,4530,COMPLETE, 8,2013-07-25 00:00:00.0,2911,PROCESSING, 9,2013-07-25 00:00:00.0,5657,PENDING_PAYMENT, 10,2013-07-25 00:00:00.0,5648,PENDING_PAYMENT)

scala> orders.take(10).foreach(println)
1,2013-07-25 00:00:00.0,11599,CLOSED
2,2013-07-25 00:00:00.0,256,PENDING_PAYMENT
3,2013-07-25 00:00:00.0,12111,COMPLETE
4,2013-07-25 00:00:00.0,8827,CLOSED
5,2013-07-25 00:00:00.0,11318,COMPLETE
6,2013-07-25 00:00:00.0,7130,COMPLETE
7,2013-07-25 00:00:00.0,4530,COMPLETE
8,2013-07-25 00:00:00.0,2911,PROCESSING
9,2013-07-25 00:00:00.0,5657,PENDING_PAYMENT
10,2013-07-25 00:00:00.0,5648,PENDING_PAYMENT

scala> orders.collect
res2: Array[String] = Array(1,2013-07-25 00:00:00.0,11599,CLOSED, 2,2013-07-25 00:00:00.0,256,PENDING_PAYMENT, 3,2013-07-25 00:00:00.0,12111,COMPLETE, 4,2013-07-25 00:00:00.0,8827,CLOSED, 5,2013-07-25 00:00:00.0,11318,COMPLETE, 6,2013-07-25 00:00:00.0,7130,COMPLETE, 7,2013-07-25 00:00:00.0,4530,COMPLETE, 8,2013-07-25 00:00:00.0,2911,PROCESSING, 9,2013-07-25 00:00:00.0,5657,PENDING_PAYMENT, 10,2013-07-25 00:00:00.0,5648,PENDING_PAYMENT, 11,2013-07-25 00:00:00.0,918,PAYMENT_REVIEW, 12,2013-07-25 00:00:00.0,1837,CLOSED, 13,2013-07-25 00:00:00.0,9149,PENDING_PAYMENT, 14,2013-07-25 00:00:00.0,9842,PROCESSING, 15,2013-07-25 00:00:00.0,2568,COMPLETE, 16,2013-07-25 00:00:00.0,7276,PENDING_PAYMENT, 17,2013-07-25 00:00:00.0,2667,COMPLETE, 18,2013-07-25 00:00:00.0,1205,CLOSED, 19,2013-07-25 00:00:...
scala> orders.count
res3: Long = 68883

scala> orders
res4: org.apache.spark.rdd.RDD[String] = datasets/orders MapPartitionsRDD[1] at textFile at <console>:27

scala> orders.first
res5: String = 1,2013-07-25 00:00:00.0,11599,CLOSED


Tranformations in Spark
------------------------
Higher Order Functions

function1(function2)

Wordcount using Spark
---------------------

scala> val book = sc.textFile("book.txt")
book: org.apache.spark.rdd.RDD[String] = book.txt MapPartitionsRDD[12] at textFile at <console>:27

scala> book.collect
res20: Array[String] = Array("The Project Gutenberg EBook of The Outline of Science, Vol. 1 (of 4), by ", J. Arthur Thomson, "", This eBook is for the use of anyone anywhere at no cost and with, almost no restrictions whatsoever.  You may copy it, give it away or, re-use it under the terms of the Project Gutenberg License included, with this eBook or online at www.gutenberg.org, "", "", Title: The Outline of Science, Vol. 1 (of 4), "       A Plain Story Simply Told", "", Author: J. Arthur Thomson, "", Release Date: January 22, 2007 [EBook #20417], "", Language: English, "", "", *** START OF THIS PROJECT GUTENBERG EBOOK OUTLINE OF SCIENCE ***, "", "", "", "", Produced by Brian Janes, Leonard Johnson and the Online, Distributed Proofreading Team at http://www.pgdp.net, "", "", "", "", "",...
scala>

scala> val a = book.map(x => x.split(" "))
a: org.apache.spark.rdd.RDD[Array[String]] = MapPartitionsRDD[13] at map at <console>:31

scala> a.collect
res21: Array[Array[String]] = Array(Array(The, Project, Gutenberg, EBook, of, The, Outline, of, Science,, Vol., 1, (of, 4),, by), Array(J., Arthur, Thomson), Array(""), Array(This, eBook, is, for, the, use, of, anyone, anywhere, at, no, cost, and, with), Array(almost, no, restrictions, whatsoever., "", You, may, copy, it,, give, it, away, or), Array(re-use, it, under, the, terms, of, the, Project, Gutenberg, License, included), Array(with, this, eBook, or, online, at, www.gutenberg.org), Array(""), Array(""), Array(Title:, The, Outline, of, Science,, Vol., 1, (of, 4)), Array("", "", "", "", "", "", "", A, Plain, Story, Simply, Told), Array(""), Array(Author:, J., Arthur, Thomson), Array(""), Array(Release, Date:, January, 22,, 2007, [EBook, #20417]), Array(""), Array(Language:, English)...
scala> val a = book.flatMap(x => x.split(" "))
a: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[14] at flatMap at <console>:31

scala> a.collect
res22: Array[String] = Array(The, Project, Gutenberg, EBook, of, The, Outline, of, Science,, Vol., 1, (of, 4),, by, J., Arthur, Thomson, "", This, eBook, is, for, the, use, of, anyone, anywhere, at, no, cost, and, with, almost, no, restrictions, whatsoever., "", You, may, copy, it,, give, it, away, or, re-use, it, under, the, terms, of, the, Project, Gutenberg, License, included, with, this, eBook, or, online, at, www.gutenberg.org, "", "", Title:, The, Outline, of, Science,, Vol., 1, (of, 4), "", "", "", "", "", "", "", A, Plain, Story, Simply, Told, "", Author:, J., Arthur, Thomson, "", Release, Date:, January, 22,, 2007, [EBook, #20417], "", Language:, English, "", "", ***, START, OF, THIS, PROJECT, GUTENBERG, EBOOK, OUTLINE, OF, SCIENCE, ***, "", "", "", "", Produced, by, Brian, Jan...
scala> val b = a.map(i => (i,1))
b: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[15] at map at <console>:33

scala> b.collect
res24: Array[(String, Int)] = Array((The,1), (Project,1), (Gutenberg,1), (EBook,1), (of,1), (The,1), (Outline,1), (of,1), (Science,,1), (Vol.,1), (1,1), ((of,1), (4),,1), (by,1), (J.,1), (Arthur,1), (Thomson,1), ("",1), (This,1), (eBook,1), (is,1), (for,1), (the,1), (use,1), (of,1), (anyone,1), (anywhere,1), (at,1), (no,1), (cost,1), (and,1), (with,1), (almost,1), (no,1), (restrictions,1), (whatsoever.,1), ("",1), (You,1), (may,1), (copy,1), (it,,1), (give,1), (it,1), (away,1), (or,1), (re-use,1), (it,1), (under,1), (the,1), (terms,1), (of,1), (the,1), (Project,1), (Gutenberg,1), (License,1), (included,1), (with,1), (this,1), (eBook,1), (or,1), (online,1), (at,1), (www.gutenberg.org,1), ("",1), ("",1), (Title:,1), (The,1), (Outline,1), (of,1), (Science,,1), (Vol.,1), (1,1), ((of,1), (4)...
scala> val c = b.reduceByKey((x,y) => (x + y))
c: org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[16] at reduceByKey at <console>:37

scala> c.collect
res25: Array[(String, Int)] = Array((intimately,2), (rises.,2), (egg-shell,1), (bone,3), (INCREASE,2), (INCONCEIVABLY,2), (winter;,1), (ducklings,1), (chapters,1), (hunter,,1), (spirited,1), (preventing,2), (pigeon,4), (auroral,1), (been,260), (fuller,3), (spice,1), (lamp-shells,,2), (Dividing,1), (accomplished,2), (knows,7), (inch.,5), (_Jurassic_,1), (Uranus.,1), (peoples,,2), (tips,2), (2.,11), (Western,1), (smooth,4), (Encyclopï¿½dia,,1), (angels,,1), (influences.,1), (luminous,12), (performed,,1), (8,,1), (audibly,1), (well-being,1), (swamp,2), (robin,1), (erected,1), (Folk,1), (178,2), (apple--out,1), (dead,15), (bank.,1), (startling,4), (imitation,,1), (thus,28), (air-tubes,2), (us.",1), (rabbit.,1), (descending,1), (kin-sympathy,1), (constant,,2), (iron,20), (271,2), (waste-produc...


THE LOGIC
---------

val c = b.reduceByKey((x,y) => (x + y))

b = (raghu,1)(raghu,1)(raghu,1)
               KEY     VALUE
reduceByKey = (raghu, (1,1,1))

((x,y) => (x + y))

Take any two values x and y and keep on adding them until all the values are over.

Here, x and y are values.
                       x y
reduceByKey = (raghu, (1,1,1))

        x y
(raghu,(2,1))

((x,y) => (x + y))

(raghu,3)

scala> book.flatMap(x => x.split(" ")).map(i => (i,1)).reduceByKey((x,y) => (x + y)).collect
res26: Array[(String, Int)] = Array((intimately,2), (rises.,2), (egg-shell,1), (bone,3), (INCREASE,2), (INCONCEIVABLY,2), (winter;,1), (ducklings,1), (chapters,1), (hunter,,1), (spirited,1), (preventing,2), (pigeon,4), (auroral,1), (been,260), (fuller,3), (spice,1), (lamp-shells,,2), (Dividing,1), (accomplished,2), (knows,7), (inch.,5), (_Jurassic_,1), (Uranus.,1), (peoples,,2), (tips,2), (2.,11), (Western,1), (smooth,4), (Encyclopï¿½dia,,1), (angels,,1), (influences.,1), (luminous,12), (performed,,1), (8,,1), (audibly,1), (well-being,1), (swamp,2), (robin,1), (erected,1), (Folk,1), (178,2), (apple--out,1), (dead,15), (bank.,1), (startling,4), (imitation,,1), (thus,28), (air-tubes,2), (us.",1), (rabbit.,1), (descending,1), (kin-sympathy,1), (constant,,2), (iron,20), (271,2), (waste-produc...

//Wordcount in a better way

book.flatMap(x => x.split("\\W")).map(i => (i,1)).reduceByKey((x,y) => (x + y)).collect

//What if I want to sort the output based on the key (ascending)

book.flatMap(x => x.split("\\W")).map(i => (i,1)).reduceByKey((x,y) => (x + y)).sortBy(_._1).collect

sortBy will sort the data in ascending order (default)

sortBy(_._1)

The first _ represents each element in the RDD = (key, value)

._1 means the first element in (key,value) which is nothing but the key.

//What if I want to sort the output based on the key (descending)

book.flatMap(x => x.split("\\W")).map(i => (i,1)).reduceByKey((x,y) => (x + y)).sortBy(_._1, false).collect

// Find out the most repeating words

scala> book.flatMap(x => x.split("\\W")).map(i => (i,1)).reduceByKey((x,y) => (x + y)).sortBy(_._2, false).collect
res36: Array[(String, Int)] = Array(("",27635), (the,7983), (of,5444), (and,2798), (a,2474), (to,2218), (is,2116), (in,2074), (that,1317), (it,989), (are,943), (The,936), (which,827), (as,763), (be,676), (on,668), (by,646), (or,646), (we,610), (with,597), (from,574), (for,534), (have,477), (there,451), (was,445), (has,437), (an,425), (not,418), (this,413), (its,403), (It,402), (at,395), (THE,384), (they,370), (but,352), (one,342), (A,339), (OF,307), (more,296), (their,291), (very,291), (may,291), (s,285), (all,274), (been,264), (some,260), (like,249), (so,238), (than,237), (were,236), (Illustration,235), (these,231), (great,230), (other,227), (In,227), (earth,220), (no,216), (when,213), (into,211), (animals,210), (sun,207), (our,207), (man,197), (many,196), (about,188), (light,187), (wa...

//There is a transformation called sortByKey

book.flatMap(x => x.split("\\W")).map(i => (i,1)).reduceByKey((x,y) => (x + y)).sortByKey(false).collect

//How can I filter the data???

1. I want to find words from the book matching with the word "worm"

book.flatMap(x => x.split("\\W")).map(i => (i,1)).reduceByKey((x,y) => (x + y)).filter(_._1.contains("worm")).collect

2. I want to find the exact word "worm"

book.flatMap(x => x.split("\\W")).map(i => (i,1)).reduceByKey((x,y) => (x + y)).filter(_._1.equals("worm")).collect

3. I want to find out all the words which are repeating more than 1000 times.

book.flatMap(x => x.split("\\W")).map(i => (i,1)).reduceByKey((x,y) => (x + y)).filter(_._2 > 1000).collect

4. I want to find out all the words which are having more than 9 letters 

book.flatMap(x => x.split("\\W")).map(i => (i,1)).reduceByKey((x,y) => (x + y)).filter(_._1.length > 9).collect

5. I want to find out all words starting with W

book.flatMap(x => x.split("\\W")).map(i => (i,1)).reduceByKey((x,y) => (x + y)).filter(_._1.startsWith("W")).collect

Two to Tango
------------
Coalesce and repartition

Coalesce is used if you want to decrease the number of partitions on an RDD
Repartition is used if you want to either decrease or increase the number of partitions

In order to decrease the number of partitions you can use either coalesce or repartition. But we always prefer coalesce.

IN ORDER TO INCREASE THE NUMBER OF PARTITIONS, THERE IS ONLY ONE WAY. REPARTITION.
TO DECREASE, YOU HAVE 2 OPTION. REPARTITION OR COALESCE. BUT PREFER COALESCE.


How to bring up your own Databricks cluster in cloud
====================================================

Caching an RDD
==============

cache() ===> cache the whole RDD in RAM
persist()===> By default stores the whole RDD in RAM, but you can change the storage levels
unpersist()===> Remove the cached RDD

Friends By Age Problem
===================================================
SN  Name    Age No.of friends
---------------------------------------------------
0	Will	33	385
1	Jean-Lu	26	2
2	Hugh	55	221
3	Deanna	40	465
4	Quark	68	21
5	Weyoun	59	318
6	Gowron	37	220
49	Ezri	40	254
120	Jean-Lu	68	264




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

    rdd.collect                    K  V     K  V    K   V
res14: Array[(Int, Int)] = Array((33,385), (26,2), (55,221), (40,465), (68,21), (59,318), (37,220), (54,307), (38,380), (27,181), (53,191), (57,372), (54,253), (56,444), (43,49), (36,49), (22,323), (35,13), (45,455), (60,246), (67,220), (19,268), (30,72), (51,271), (25,1), (21,445), (22,100), (42,363), (49,476), (48,364), (50,175), (39,161), (26,281), (53,197), (43,249), (27,305), (32,81), (58,21), (64,65), (31,192), (52,413), (67,167), (54,75), (58,345), (35,244), (52,77), (25,96), (24,49), (20,1), (40,254), (51,283), (36,212), (19,269), (62,31), (19,5), (41,278), (44,194), (57,294), (59,158), (59,284), (20,100), (62,442), (69,9), (58,54), (31,15), (52,169), (21,477), (48,135), (33,74), (30,204), (52,393), (45,184), (22,179), (20,384), (65,208), (40,459), (62,201), (40,407), (61,337), ...

    
    // Lots going on here...
    // We are starting with an RDD of form (age, numFriends) where age is the KEY and numFriends is the VALUE
    // We use mapValues to convert each numFriends value to a tuple of (numFriends, 1)
    // Then we use reduceByKey to sum up the total numFriends and total instances for each age, by
    // adding together all the numFriends values and 1's respectively.
    val totalsByAge = rdd.mapValues(x => (x, 1)).reduceByKey( (x,y) => (x._1 + y._1, x._2 + y._2))

    rdd.mapValues(x => (x,1)).collect

    res15: Array[(Int, (Int, Int))] = Array((33,(385,1)), (26,(2,1)), (55,(221,1)), (40,(465,1)), (68,(21,1)), (59,(318,1)), (37,(220,1)), (54,(307,1)), (38,(380,1)), (27,(181,1)), (53,(191,1)), (57,(372,1)), (54,(253,1)), (56,(444,1)), (43,(49,1)), (36,(49,1)), (22,(323,1)), (35,(13,1)), (45,(455,1)), (60,(246,1)), (67,(220,1)), (19,(268,1)), (30,(72,1)), (51,(271,1)), (25,(1,1)), (21,(445,1)), (22,(100,1)), (42,(363,1)), (49,(476,1)), (48,(364,1)), (50,(175,1)), (39,(161,1)), (26,(281,1)), (53,(197,1)), (43,(249,1)), (27,(305,1)), (32,(81,1)), (58,(21,1)), (64,(65,1)), (31,(192,1)), (52,(413,1)), (67,(167,1)), (54,(75,1)), (58,(345,1)), (35,(244,1)), (52,(77,1)), (25,(96,1)), (24,(49,1)), (20,(1,1)), (40,(254,1)), (51,(283,1)), (36,(212,1)), (19,(269,1)), (62,(31,1)), (19,(5,1)), (41,(278...
      K    V
    (54,(307,1))
      K    V
    (54,(75,1))
      K    V
    (54,(253,1))

   reduceByKey( (x,y) => (x._1 + y._1, x._2 + y._2))

   (54, ((307,1), (75,1), (253,1) )

   First Iteration
            x       y
   (54, ((307,1), (75,1), (253,1) )
               307    75   1     1
   (x,y) => (x._1 + y._1, x._2 + y._2)

   (54, (382,2), (253,1))

   Second iteration
             x       y
     (54, (382,2), (253,1))
                  382   253   1      1
      (x,y) => (x._1 + y._1, x._2 + y._2)

      (635,3)
/*---------------------------------------------------------------------------*/
So now we have tuples of (age, (totalFriends, totalInstances))
To compute the average we divide totalFriends / totalInstances for each age.
/*---------------------------------------------------------------------------*/

    val averagesByAge = totalsByAge.mapValues(x => x._1 / x._2)
    averagesByAge.collect.foreach(println)


    Job Vs Stage Vs Task

    A job is the complete DAG that is formed after calling an action.

    A job is divided into stages

    Stages contain tasks.

    There are two types of transformations.

    Narrow transformation

    Examples are map, flatMap, filter etc.....

     DN-1                       DN-2                       DN-3
     raghu,1                     raghu,1                      raghu,1
     ram,1                       ram,1                        ram,1

     map(x => (x,1)). In order to run this map transformstion, we do not need any data movement between the nodes.

 Wide transformation
 -------------------
 Examples are reduceByKey, join, groupByKey

     DN-1                       DN-2                          DN-3
     raghu,1                    raghu,1                       raghu,1
     ram,1                      ram,1                         ram,1


      reduceByKey((x,y) => (x + y))

      raghu(1,1,1)
      ram(1,1,1)

        DN-1                       DN-2                          DN-3
       raghu,1,1,1
       ram,1,1,1

SPARK WILL DECIDE STAGES BASED ON WIDE TRANSFORMATIONS. MEANING, WHENEVER THERE IS A WIDE TRANSFORMATION IN YOUR CODE, A STAGE IS CREATED.

SPARK SQL
----------
What is the real use case of Spark? Is it really a tool to handle unstructured data?

Spark SQL allows you to give structure to the data.

Core Spark                                          SparkSQL

SparkContext                                        SQLContext : HiveContext

RDD                                                 Dataframe

Transformations and Actions                         SQL, HQL, Language Integrated Queries

Dataframes are much much more optimised than RDDs. They take very less space in memory as well.



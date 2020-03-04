package com.fis.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._

object Rdd {

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

		/*-----------------------------------map(func)-------------------------------------*/
		val x = spark.sparkContext.parallelize(List("spark", "rdd", "example", "sample", "example"), 3)
		val y = x.map(x => (x,1))
		y.foreach(println)
		println(" ")


		// rdd y can be re writen with shorter syntax in scala as 
		val z = x.map((_,1))
		z.foreach(println)
		println(" ")

		val a = spark.sparkContext.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
		val b = a.map(x => (x.length, x))
		b.values.foreach(println)
		b.keys.foreach(println)
		println(" ")

		/*-----------------------------------flatMap(func)-------------------------------------*/
		//"Similar to map, but each input item can be mapped to 0 or more output items (so func should return a Seq rather than a single item)."
		val data = spark.sparkContext.parallelize(List(1,2,3)).flatMap(x => List(x,x,x)).collect
		data.foreach(println)
		println(" ")

		//Same using map
		val data_map = spark.sparkContext.parallelize(List(1,2,3)).map(x => List(x,x,x)).collect
		data_map.foreach(println)
		println(" ")

		//Example2
		val names = spark.sparkContext.parallelize(List("joe admon john smith mith bob")).map(_.split(" ")).collect
		names.foreach(println)
		println(" ")

		val names_fmap = spark.sparkContext.parallelize(List("joe admon john smith mith bob")).flatMap(_.split(" ")).collect
		names_fmap.foreach(println)
		println(" ")

		/*-----------------------------------filter------------------------------------*/

		val names_fil = spark.sparkContext.parallelize(List("Spark","Hadoop","MapReduce","Pig","Spark2","Hadoop2","Pig")).filter(_.equals("Spark")).collect
		names_fil.foreach(println)
		println(" ")

		val names_fil1 = spark.sparkContext.parallelize(List("Spark","Hadoop","MapReduce","Pig","Spark2","Hadoop2","Pig")).filter(_.contains("Spark")).collect
		names_fil1.foreach(println)
		println(" ")

		val names_tup = spark.sparkContext.parallelize(List((4,"Spark"),(3,"Hadoop"),(5,"MapReduce"),(2,"Pig"),(1,"Spark2"),(10,"Hadoop2"),(8,"Pig")))
		val res1 = names_tup.filter(_._2.contains("Spark")).filter(_._1<2).collect
		val res2 = names_tup.sortByKey().filterByRange(1,4).collect
		println("-----Res1-----")
		res1.foreach(println)
		println("-----Res2------")
		res2.foreach(println)

		/*----------------------------------sample(withreplacement:boolean,fraction:Double,seed:Long)-------------------*/
		/*when you are taking sample from RDD 
		withReplacement - can elements be sampled multiple times (replaced when sampled out)
		fraction - expected size of the sample as a fraction of this RDD's size 
		without replacement: probability that each element is chosen; 
		fraction must be [0, 1] with replacement: expected number of times each element is chosen; fraction must be >= 0
		seed - seed for the random number generator*/

		val parallel = spark.sparkContext.parallelize(1 to 9)
		parallel.sample(true,.2).count

		//parallel.sample(true,.2).doesn't return the same sample size: that's because spark internally uses something called Bernoulli sampling for taking the sample. 

		val rdd1 = spark.sparkContext.parallelize(1 to 10)
		val s = rdd1.sample(true,.4,4).collect
		println("------Printing s------")
		s.foreach(println)

		//seed guarantee is that if you start from the same seed you will get the same sequence of numbers

		/*-------------------------------------union(a different rdd)-------------------------*/
		val rdd2 = spark.sparkContext.parallelize(1 to 9)
		val rdd3 = spark.sparkContext.parallelize(5 to 15)
		val rdd4 = rdd2.union(rdd3)
		println(" ")
		println("------rdd4------")
		rdd4.foreach(println)
		/*-------------------------------------intersection(a different rdd)-------------------------*/ 
		val rdd5 = rdd2.intersection(rdd3)
		println(" ")
		println("------rdd5------")
		rdd5.foreach(println)

		/*-------------------------------------Distinct -------------------------------------*/ 
		println(" ")
		println("------Distinct------")
		rdd4.distinct().foreach(println)

		/*-------------------------------------sortBy -------------------------------------------*/ 
		println(" ")
		println("------SortBy------")
		val rdd6 =rdd4.sortBy(c => c,false).collect
		rdd6.foreach(println)

		val rdd7 = spark.sparkContext.parallelize(Array(("H", 10), ("A", 26), ("Z", 1), ("L", 5)))
		val rdd8 = rdd7.sortBy(_._2, false).collect
		println("------rdd8------")
		rdd8.foreach(println)
		rdd7.sortBy(_._1, false).collect

		/*-----------------------------mapPartitions(func)------------------------------------*/
		/*> mapPartitions() can be used as an alternative to map() and foreach() .
		> mapPartitions() can be called for each partitions while map() and foreach() is called for each elements in an RDD
		> Hence one can do the initialization on per-partition basis rather than each element basis*/
		var rdd9 = spark.sparkContext.parallelize(1 to 9, 3)
		var rdd10 = rdd9.mapPartitions(x=>List(x.next).iterator).collect()
		println("------rdd10------")
		rdd10.foreach(println)

		/*----------------------------------mapPartitionsWithIndex------------------------------*/
		//Return a new RDD by applying a function to each partition of this RDD, while tracking the index of the original partition.
		val nums = spark.sparkContext.parallelize(List(1,2,3,4,5,6), 2)
		def myfunc(index: Int, iter: Iterator[(Int)]) : Iterator[String] = {
		iter.toList.map(x => "[index:" + index + ", val: " + x + "]").iterator
		}
		val rdd11 = nums.mapPartitionsWithIndex(myfunc).collect
		println("------rdd11------")
		rdd11.foreach(println)

		/*--------------------------------------groupBy , keyBy-----------------------------------------------------*/
		val rdd = spark.sparkContext.parallelize(List((1,"apple"),(2,"banana"),(3,"lemon"),(4,"apple"),(1,"lemon")) )
		println("------groupBy------")
		val res3 = rdd.groupBy(_._1).collect()
		res3.foreach(println)
		println("------keyBy------")
		val res4 =rdd.keyBy(_._1).collect
		res4.foreach(println)
		/*----------------------------------------zip, zipwithIndex--------------------------------------------------
		zip: Returns a list formed from this list and another iterable collection by combining 
		corresponding elements in pairs. If one of the two collections is longer than the 
		other, its remaining elements are ignored.


		//zipWithIndex : Zips this list with its indices.

		Returns: A new list containing pairs consisting of all elements of this list 
		paired with their index. Indices start at 0.*/
		val odds = spark.sparkContext.parallelize(List(1,3,5))
		val evens = spark.sparkContext.parallelize(List(2,4,6))
		val numbers = odds zip evens
		println("------Zipped Numbers------")
		numbers.collect().foreach(println)
		println("------Zipped with index evens------")
		val evenswithIndex = evens.zipWithIndex 
		evenswithIndex.collect().foreach(println)

		/*------------------------------------------colease/repartition--------------------------------------------*/
		/* > repartition: The repartition method can be used to either increase or decrease the number of partitions in a DataFrame.
		> coalesce :The coalesce method reduces the number of partitions in a DataFrame. */
		val rdd12 = spark.sparkContext.parallelize( 1 to 100,2)
		println(rdd12.partitions.size)
		rdd12.foreachPartition(x => println(x.toList))
		val numberIncreased = rdd12.repartition(10)
		println(numberIncreased.partitions.size)
		numberIncreased.foreachPartition(x => println(x.toList))
		val numberIncreasedCo = numberIncreased.coalesce(3)
		numberIncreasedCo.foreachPartition(x => println(x.toList))

		/*-----------------------RDD Actions----------------------------------------------------------------

		Unlike Transformations which produce RDDs, action functions produce a value back to the Spark driver program. 
		Actions may trigger a previously constructed, lazy RDD to be evaluated.
		----------------------------------------------------------------------------------------------------- */
		//Reduce
		val d = spark.sparkContext.parallelize(1 to 10, 3)
		println(d.reduce(_+_))
		val names1 = spark.sparkContext.parallelize(List("abe", "abby", "apple"))
		println(names1.flatMap(k => List(k.size) ).reduce((t1,t2) => t1 + t2))
		//Another way of writing the same
		println(names1.flatMap(x=>List(x.size)).reduce(_+_))
		println("reduce action on strings")
		println(names1.reduce((t1,t2) => t1 + t2))
		val names2 = spark.sparkContext.parallelize(List("apple", "beatty", "beatrice")).map(a => (a, a.size))
		names2.foreach(println)
		println(names2.flatMap(t => Array(t._2)).reduce(_ + _))

		//First
		println(names2.first)

		//takeOrdered: It will return a Array with given numbers of ordered values in the RDD
		val nums_1 = spark.sparkContext.parallelize(List(1,5,3,9,4,0,2))
		println("---ordered take----")
		nums_1.takeOrdered(4).foreach(println)

		////take(n):It will return a Array with first given numbers of values in the RDD
		println("---take----")
		nums_1.take(4).foreach(println)
		println("")
		//count():It will return a Long value that indicates how many elements presented in the RDD.
		println(nums_1.count)
		val c = spark.sparkContext.parallelize(List((3, "Gnu"), (3, "Yak"), (5, "Mouse"), (3, "Dog")), 2)
		println(c.countByKey)
		val e = spark.sparkContext.parallelize(List(1,2,3,4,5,6,7,8,2,4,2,1,1,1,1,1))
		println(e.countByValue)

		//collect(func)collect returns the elements of the dataset as an array back to the driver program.collect is often used in previously provided examples such as Spark Transformation Examples in order to show the values of the return.
		val f = spark.sparkContext.parallelize(List("Gnu", "Cat", "Rat", "Dog", "Gnu", "Rat"), 2)
		f.collect()

		//collectAsMap: It will return A Map object containg all key value pairs converted as Map.
		val alphanumerics = spark.sparkContext.parallelize(List((1,"a"),(2,"b"),(3,"c")))
		println(alphanumerics.collectAsMap)

		//saveAsTextFile(filepath):Write out the elements of the data set as a text file in a filepath directory on the filesystem, HDFS or any other Hadoop-supported file system.
		//val h = spark.sparkContext.parallelize(1 to 10000, 3)
		//h.saveAsTextFile("mydata_a")
		//val g = spark.sparkContext.parallelize(List(1,2,3,4,5,6,6,7,9,8,10,21), 3)
		//g.saveAsTextFile("hdfs://localhost:8020/user/hadoop/test");

		//foreachPartition:Executes an parameterless function for each partition. Access to the data items contained in the partition is provided via the iterator argument
		val i = spark.sparkContext.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9), 3)
		i.foreachPartition(x => println(x.reduce(_ + _)))

		//top(n)
		val j = spark.sparkContext.parallelize(Array(6, 9, 4, 7, 5, 8), 2)
		println(" ")
		println("Top 2")
		println(j.top(2).foreach(println))
		//Max,Min,Sum,Mean,Variance,stdev
		val numbers2 = spark.sparkContext.parallelize(1 to 100)
		println("sum:"+ numbers2.sum)
		println("Max:"+ numbers2.max)
		println("Min:"+ numbers2.min)
		println("Mean:"+ numbers2.mean)
		println("Variance:"+ numbers2.variance)
		println("stdev:"+ numbers2.stdev)


	}

}
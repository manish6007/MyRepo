SparkSQL: 
------------
It is library to process structured and semi structured data. SparkSQL extends RDD to a DataFrame object.

Hive Vs SparkSQL:
--------------------

Limitation of Hive:
1. Hive queries are converted in MR jobs so they are quite slow.

2. Hive doesn't have resume capability. If the processing dies in the middle of a workflow, You cannot resume from where it got stuck.

3. Realtime/ Inreractive queries cannot be run on Hive.


Ways to interact with Spark SQL:
1. SQL
2. DataFrame API
3. Dataset API

DataFrame:
----------
It is distributed collection of data organised into named columns like table. It
- Contains row object
- Can run sql queries
- Has schema (leading to more efficient storage)
- Read and write to JSON, Hive, parquet
- Communicate with JDBC/ODBC, Tableau

Features:
------------

Hive compatible:
-----------------
- Can run hive queries on existing warehouse.
- Uses Hive frontend amd metastore, giving full compatibility over hive data queries and UDFs.
- Hive warehouse can be queried using spark sql engine suing hive metastore.

Optimization using catalyst optimizer.
--------------------------------------
Optimization is done in 4 phases.
1. Analysing a logical plan
2. Logical plan optimization
3. Physical planning
4. Code generation to compile parts of query to java bytecode.

SparkSQL queries will be faster than RDD based queries beacuse of catalyst optimizer.

SQL
DataFrame-->Query Plan(Cost plan)-->Optimized query plan(least cost plan)-->RDD


Dataframes are immutable as dataframe is a layer on top of RDD.
---------------------------------------------------------------

If we collect() on dataframe we will get apache.spark.sql.row


Spark 1 Vs Spark 2:
--------------------
 - Spark 1 is using the SQLContext whereas Spark 2 uses SparkSession Object.
 - Spark 1 have Hive context to interact with Hive whereas Spark Session itself can interact with hive.

SparkSQL is used to analyse the data(query engine) not for storing the data.

For getting the benefit of indexing in Hive we save the data as ORC. We can use the Hive to store data and spark for querying. 

Creating DataFrame:
--------------------------

1) Infer schema using reflection: This technique uses case class for reflection.
2) Pragamatically specifying the schema
    - Create row based RDD.
    - Use struc type for schema
    - Use createDataframe to create dataframe

Note:
-----
   - We use struct type as case class is having a limilation of having maximum 32 columns. struct type can have any number of columns
   - On the fly schema can be defined in structtype.
   - RDD needs to be converted in the sql row before creating the dataframe

Example:
--------

    import org.apache.spark.sql.types._

    import org.apache.spark.sql.{Row, SparkSession}

    val schema = StructType(Array(StructField("name",StringType,true),StructField("age",IntegerType,true)))

    val data = sc.parallelize( Seq("john","Adom","Smith")).map(x => (x,20+x.length))

    val rowRDD = data.map(x => Row(x._1,x._2))	

    val df = spark.createDataFrame(rowRDD,schema)

    df.createOrReplaceTempView("people")

    spark.sql("select * from people").show


Reading CSV data using databricks csv packages
----------------------------------------------------------------------
   Download the jar and place it in jars folder under spark installation /get the maven co-ordinates from      https://mvnrepository.com/artifact/com.databricks/spark-csv_2.11/1.5.0

  Or 
  Start the shell using below command.

  spark-shell --packages com.databricks:spark-csv_2.11:1.5.0

  val characters_df = spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", ",").load("/user/support1161/navya/StarWars.csv")

  characters_df.show

  Dealing with xml files
  ---------------------

  To read xml files you need to restart your spark-shell with the below given arguments

  spark-shell --packages com.databricks:spark-xml_2.11:0.4.1

		OR
  Download the jar and place it in jars folder under spark installation /get the maven co-ordinates from 		
  https://mvnrepository.com/artifact/com.databricks/spark-xml_2.11/0.4.1

  val employees_df = spark.read.format("com.databricks.spark.xml").option("inferSchema",   "true").option("rootTag","employees").option("rowTag","employee").load("C:/Users/e1091444/Desktop/Spark/SparkSQL/class_material26thmay/d  atasets/datasets/employees.xml")

  val emp_dataNormal = employees_df.select("emp_no","emp_name","address.city","address.country","address.pincode","salary","dept_no").show

Dealing with parquet files
--------------------------
  val baby_names_df = spark.read.parquet("C:/Users/e1091444/Desktop/Spark/SparkSQL/class_material26thmay/datasets/datasets/baby_names.parquet")
baby_names_df.show


Dataset:
--------
  - A DataFrame is really just a dataset of row objects(Dataset[Row])
  - Datasets can explicitely wrap a given struct or type
	Dataset[Person], Dataset[(String, Double)])
	It knows what its columns are from the get go.
  - Dataframe's schema is inferred at run time; But the dataset can be inferred at complite time 
  - Faster detection of errors, and better optimization
  - RDD's can be converted to DataSets with .toDS()

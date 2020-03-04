
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical.{HintInfo, ResolvedHint}
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.classification.LogisticRegression
import spark.implicits._
import org.apache.log4j._
Logger.getLogger("org").setLevel(Level.ERROR)

object CityBikeLR {

def main(args: Array[String]) {

val conf = new SparkConf().setAppName("MovieLensALS").setMaster("local[2]")

val sc = new SparkContext(conf)

val spark = SparkSession.builder().getOrCreate()

//Creating Case Class
case class Location(lat: Double, lon: Double)

//Creating trait
trait DistanceCalcular {
  def calculateDistanceInKilometer(userLocation: Location, warehouseLocation: Location): Int
}

//Creating class
class DistanceCalculatorImpl extends DistanceCalcular {

  private val AVERAGE_RADIUS_OF_EARTH_KM = 6371

  override def calculateDistanceInKilometer(userLocation: Location, warehouseLocation: Location): Int = {
    val latDistance = Math.toRadians(userLocation.lat - warehouseLocation.lat)
    val lngDistance = Math.toRadians(userLocation.lon - warehouseLocation.lon)
    val sinLat = Math.sin(latDistance / 2)
    val sinLng = Math.sin(lngDistance / 2)
    val a = sinLat * sinLat +
      (Math.cos(Math.toRadians(userLocation.lat))
	  * Math.cos(Math.toRadians(warehouseLocation.lat))
        * sinLng * sinLng)
    val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
    (AVERAGE_RADIUS_OF_EARTH_KM * c).toInt
  }
}

//new DistanceCalculatorImpl().calculateDistanceInKilometer(Location(10,20),Location(40,20))

//val Distance = new DistanceCalculatorImpl()

def distance(lat1:Double,long1:Double,lat2:Double,long2:Double):Double={
val dist = new DistanceCalculatorImpl()
dist.calculateDistanceInKilometer(Location(lat1,long1),Location(lat2,long2))
}

 val distanceUDF = udf[Double,Double,Double,Double,Double](distance)
 spark.sqlContext.udf.register("distanceUdf",distanceUDF)
 

val customSchema=StructType(Array(StructField("tripduration",DoubleType,true),StructField("starttime",DateType,true),StructField("stoptime",DateType,true),StructField("start_station_id",IntegerType,true),StructField("start_station_name",StringType,true),StructField("start_station_latitude",DoubleType,true),StructField("start_station_longitude",DoubleType,true),StructField("end_station_id",IntegerType,true),StructField("end_station_name",StringType,true),StructField("end_station_latitude",DoubleType,true),StructField("end_station_longitude",DoubleType,true),StructField("bikeid",IntegerType,true),StructField("userType",StringType,true),StructField("birth_year", IntegerType,true),StructField("gender", IntegerType,true)))


val df = spark.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter",",").schema(customSchema).load("data")

val data1 =df.withColumn("dist",expr("distanceUdf(start_station_latitude,start_station_longitude,end_station_latitude,end_station_longitude)")).show()	
 
 val data = data1.na.drop()
 data.registerTempTable("citybike_data")
 
 //Which route Citi Bikers ride the most?
 spark.sql("select end_station_name,end_station_latitude,end_station_longitude,count(1) as num_of_trips from citybike_data group by end_station_name,end_station_latitude,end_station_longitude order by count(1) desc limit(1)").show
 
 //Find the biggest trip and its duration?
  spark.sql("select tripduration,start_station_name,end_station_name,dist as distance from citybike_data order by dist desc").show(1)
 
 //how far they go
  spark.sql("select dist as distance from citybike_data order by distance desc").show(1)
 
 //When do they ride?
 spark.sql("select starttime,stoptime,dist as distance from citybike_data order by distance desc limit 1").show()
 
 //Which stations are most popular?
 spark.sql("select end_station_id,end_station_name,count(1) from citybike_data group by end_station_id,end_station_name order by count(1) desc limit 5").show()
 
 //Which days of the week are most rides taken on?
  spark.sql("select date_format(starttime,'EEEE') as day_of_week,count(1) as no_of_trips from citybike_data group by day_of_week order by no_of_trips desc limit 1").show()
  
  
  //MLLib
	val logregdata = (data.select(data("gender").as("label"),$"start_station_id",$"start_station_name",$"end_station_id",$"end_station_name",$"start_station_latitude",$"start_station_longitude",$"end_station_latitude",$"end_station_longitude",$"birth_year"))

	import org.apache.spark.ml.feature.{VectorAssembler,StringIndexer,VectorIndexer}
	import org.apache.spark.ml.linalg.Vectors
	import org.apache.spark.ml.Pipeline
	import org.apache.spark.mllib.evaluation.MulticlassMetrics

	val assembler = (new VectorAssembler().setInputCols(Array("start_station_id","end_station_id","start_station_latitude","start_station_longitude","end_station_latitude","end_station_longitude","birth_year")).setOutputCol("features"))

 
	
	val pipeline = assembler.transform(logregdata)
	val lr = new LogisticRegression().setFeaturesCol("features")
	val Array(training,test) = pipeline.randomSplit(Array(0.7,0.3),seed=12345)
	val model = lr.fit(training)
	val result = model.transform(test)
	result.show()
 
	
	val predeictionAndLablels = result.select($"prediction",$"label").as[(Double,Double)].rdd
	val metrics = new MulticlassMetrics(predeictionAndLablels)
	println("Confusion Matrix")
	println(metrics.confusionMatrix)
	}
	}
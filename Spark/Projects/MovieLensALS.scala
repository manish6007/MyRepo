import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.mllib.recommendation.{ALS,MatrixFactorizationModel, Rating}

object MovieLensALS {

def main(args: Array[String]) {

val conf = new SparkConf().setAppName("MovieLensALS").setMaster("local[2]")

val sc = new SparkContext(conf)

// input format MovieID::Title::Genres
case class Movies(movieId: Int, title: String, genres: Seq[String])

// input format is UserID::Gender::Age::Occupation::Zip-code
case class Users(userId: Int, gender: String, age: Int,occupation: String, zip: String)
//input format is UserID::MovieID::Rating::Timestamp
case class Rating(userID:Int, movieID:Int, rating:Double)

//function to parse rating data
def parseRatings(line:String):Rating={
val fields = line.split("::")
Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
}

//function to parse movie data
def parseMovie(str: String):Movies={
val fields = str.split("::")
Movies(fields(0).toInt,fields(1).toString,Seq(fields(2)))
}

//function to parse user
def parseUser(str:String):Users = {
val fields = str.split("::")
Users(fields(0).toInt, fields(1), fields(2).toInt,fields(3), fields(4))
}

//Load the data into RDD
val ratingText = sc.textFile("datasets/ratings.dat")
ratingText.first()

//creating RDD from rating
val ratingRDD = ratingText.map(parseRatings).cache()

//creating rating dataframe
val ratingDF = ratingRDD.toDF()

//creating movie dataframe
val movieDF = sc.textFile("datasets/movies.dat").map(parseMovie).toDF()

//creating userDF
val userDF = sc.textFile("datasets/users.dat").map(parseUser).toDF()

ratingDF.registerTempTable("ratings")
movieDF.registerTempTable("movies")
userDF.registerTempTable("users")


 //Find out the animated movies that are rated 4 or above
 spark.sql("select distinct title as movie from movies m join ratings r on m.movieID=r.movieID where r.rating>=4 and genres[0] like '%Animation%' or genres[1] like '%Animation%' or genres[2] like '%Animation%' ").show()
 
 //Find out the average rating for movies
 spark.sql("select movieID,avg(rating) from ratings group by movieID order by movieID").show()
 
 //Find out the titles of the best rated movies
 spark.sql("select m.title,avg(rating)rat from ratings r join movies m on r.movieID=m.movieID group by m.title order by rat desc").show()
 
 //Group the ratings by age
 //spark.sql("select age,avg(rating) from users u join ratings r on r.userID=u.userId group by age").show()
 ratingDF.join(userDF,ratingDF.col("userID")===userDF.col("userId")).groupBy("age").count().show()
 
 //Detect the gender bias on movie ratings for a genre
   spark.sql("select gender,count(1) from users u join ratings r on r.userID=u.userId join movies m on m.movieID=r.movieID where genres[0] like '%Action%' group by gender").show()
   

//Add personal ratings to the movies and train a model to provide movie recommendation based on the personal ratings


val rawData = sc.textFile("datasets/ratings.dat")


val rawRatings = rawData.map(_.split("::").take(3))

import org.apache.spark.mllib.recommendation.Rating
val ratingRDD = rawRatings.map {case Array(user,movie,rating) => Rating(user.toInt,movie.toInt,rating.toDouble)}

val model = ALS.train(ratingRDD, 20, 10, 0.01)
val topRecsForUser = model.recommendProducts(4169, 5)
val movies = sc.textFile("datasets/movies.dat")
val movieTitles=movies.map(line => line.split("::").take(2)).map(array => (array(0).toInt,array(1))).collectAsMap()
topRecsForUser.sortBy(-_.rating).take(10).map(rating => (movieTitles(rating.product), rating.rating)).foreach(println)

}
}
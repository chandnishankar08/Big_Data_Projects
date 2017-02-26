import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.ml.linalg.{Vector,Vectors}
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.ml.evaluation.RegressionEvaluator

case class Book(userId: Int, isdn: Double, rating: Float)

def mapper(line:String): Book = {
	val fields = line.split(',') 

			val book:Book = Book(fields(0).toInt, fields(1).toDouble, fields(2).toFloat)
			return book
}
// Load and parse the data
val data = sc.textFile("/FileStore/tables/vkkosad01477628717839/converted.txt")
val dataset=data.filter(x=> x!="userId,isdn,rating").map(mapper).filter(x=> x.isdn<5216561).toDF();
val Array(training, test) = dataset.randomSplit(Array(0.8, 0.2))

// Build the recommendation model using ALS

val als = new ALS()
  .setUserCol("userId")
  .setItemCol("isdn")
  .setRatingCol("rating")
 .setMaxIter(5)
  .setRegParam(0.01)


val model = als.fit(training)


val predictions = model.transform(test)

val result=predictions.rdd.map(x=> (x(0).toString().toDouble,x(2).toString().toDouble));
val metrics = new RegressionMetrics(result);  

println("Root Mean Squre error "+metrics.rootMeanSquaredError);   

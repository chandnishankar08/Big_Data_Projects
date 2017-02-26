import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.linalg.{Vector,Vectors}

// Load training data
val data = sc.textFile("/FileStore/tables/aldxtgyj1477606949493/housing.data").map(x=>(x(13).toDouble,Vectors.dense(x(0).toDouble,x(1).toDouble,x(2).toDouble,x(3).toDouble,x(4).toDouble,x(5).toDouble,x(6).toDouble,x(7).toDouble,x(8).toDouble,x(9).toDouble, x(10).toDouble,x(11).toDouble,x(12).toDouble))).toDF("label","features")

val lr = new LinearRegression()
  .setMaxIter(10)
  .setRegParam(0.3)
  .setElasticNetParam(0.8)

// Fit the model
val lrModel = lr.fit(data)

println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

val trainingSummary = lrModel.summary

println(s"numIterations: ${trainingSummary.totalIterations}")
println(s"objectiveHistory: ${trainingSummary.objectiveHistory.toList}")
trainingSummary.residuals.show()
println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
println(s"r2: ${trainingSummary.r2}")
println(s"explainedVariance: ${trainingSummary.explainedVariance}")

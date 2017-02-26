import org.apache.spark.mllib.feature.PCA
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

val data = sc.textFile("/FileStore/tables/osieke4p1477524654756/iris.data").map(line=>line.split(","))
val mlData = data.map(x =>(x(4).toString,Vectors.dense(x(0).toDouble, x(1).toDouble, x(2).toDouble,x(3).toDouble)))
val features = data.map(x => Vectors.dense(x(0).toDouble, x(1).toDouble, x(2).toDouble,x(3).toDouble))
val pca = new PCA(2).fit(features)
val projected = data.map(x => (x(4).toString, pca.transform(Vectors.dense(x(0).toDouble, x(1).toDouble, x(2).toDouble,x(3).toDouble))))

val df = sqlContext.createDataFrame(projected).toDF("label", "features")
val convertedVecDF = MLUtils.convertVectorColumnsToML(df)
val indexer = new StringIndexer()
  .setInputCol("label")
  .setOutputCol("labelIndex")
val indexed = indexer.fit(convertedVecDF).transform(convertedVecDF)
indexed.show()

val Array(trainingData, testData) = indexed.randomSplit(Array(0.7, 0.3))

val model = new DecisionTreeClassifier()
  .setLabelCol("labelIndex")
  .setFeaturesCol("features")
val trainedModel = model.fit(trainingData)

val predictions = trainedModel.transform(testData)

val evaluator = new MulticlassClassificationEvaluator()
  .setLabelCol("labelIndex")
  .setPredictionCol("prediction")
  .setMetricName("accuracy")
val accuracy = evaluator.evaluate(predictions)
println("Accuracy: " + accuracy)

val evaluator2 = new MulticlassClassificationEvaluator()
  .setLabelCol("labelIndex")
  .setPredictionCol("prediction")
  .setMetricName("f1")
val accuracy2 = evaluator2.evaluate(predictions)
println("F1 Meaure: " + accuracy2)

val evaluator3 = new MulticlassClassificationEvaluator()
  .setLabelCol("labelIndex")
  .setPredictionCol("prediction")
  .setMetricName("weightedRecall")
val accuracy3 = evaluator3.evaluate(predictions)
println("Weighted Recall: "+ accuracy3)

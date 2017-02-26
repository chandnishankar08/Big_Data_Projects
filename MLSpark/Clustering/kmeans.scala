import org.apache.spark.ml.linalg.{Vector,Vectors}
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.clustering.KMeans

val data = sc.textFile("/FileStore/tables/fuo1cwz51477614963447/Data_User_Modeling_Dataset.csv").map(line=>line.split(","))

val mlData = data.map(x =>(x(4).toString,Vectors.dense(x(0).toDouble, x(1).toDouble, x(2).toDouble,x(3).toDouble)))

val df = sqlContext.createDataFrame(mlData).toDF("label", "features")

val kmeans = new KMeans().setK(2).setSeed(1L)

val model = kmeans.fit(df)

val WSSSE = model.computeCost(df)

println(s"Within Set Sum of Squared Errors = $WSSSE")

println("Cluster Centers: ")
model.clusterCenters.foreach(println)

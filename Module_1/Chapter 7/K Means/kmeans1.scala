
// example script to load data from hdfs

// import packages

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering.{KMeans,KMeansModel}


object kmeans1 extends App
{
  // define variables

  // data file hdfs - example data
  //    Male,Suspicion of Alcohol,Weekday,12am-4am,75,30-39

  val hdfsServer = "hdfs://hc2nn.semtech-solutions.co.nz:8020"
  val hdfsPath   = "/data/spark/kmeans/"

  val dataFile   = hdfsServer + hdfsPath + "DigitalBreathTestData2013-MALE2a.csv" 

  val sparkMaster = "spark://hc2nn.semtech-solutions.co.nz:7077"
  val appName = "K Means 1"
  val conf = new SparkConf()

  conf.setMaster(sparkMaster)
  conf.setAppName(appName)

  // create the spark context

  val sparkCxt = new SparkContext(conf)

  // load raw csv data from hdfs

  val csvData = sparkCxt.textFile(dataFile)

  // split the line into a double based vector 

  val VectorData = csvData.map
  {
    csvLine =>
      Vectors.dense( csvLine.split(',').map(_.toDouble))
  }

  // initialise kmeans and set parameters

  val kMeans = new KMeans

  // these params I may modify 

  val numClusters         = 3
  val maxIterations       = 50

  // these params I will leave as default

  val initializationMode  = KMeans.K_MEANS_PARALLEL
  val numRuns             = 1
  val numEpsilon          = 1e-4

  // note - epsilon is the distance threshold for which the clusters are 
  // considered to have converged.

  kMeans.setK( numClusters ) 
  kMeans.setMaxIterations( maxIterations ) 
  kMeans.setInitializationMode( initializationMode ) 
  kMeans.setRuns( numRuns )
  kMeans.setEpsilon( numEpsilon )

  // Train a K-means model on the given set of points; data should be cached 
  // for high performance, because this is an iterative algorithm.

  VectorData.cache

  // train kmeans

  val kMeansModel = kMeans.run( VectorData )

  // Return the K-means cost (sum of squared distances of points to their nearest center) 
  // for this model on the given data.

  val kMeansCost = kMeansModel.computeCost( VectorData )

  println( "Input data rows : " + VectorData.count() )

  println( "K Means Cost    : " + kMeansCost )

  // print the centre of each cluster

  kMeansModel.clusterCenters.foreach{ println } 

  // predict the clusters for the data points 

  val clusterRddInt = kMeansModel.predict( VectorData )

  // determine a count of data points per cluster 

  val clusterCount = clusterRddInt.countByValue
 
  clusterCount.toList.foreach{ println } 

} // end object kmeans1


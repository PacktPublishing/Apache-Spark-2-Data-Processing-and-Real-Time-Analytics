
// load image test data from hdfs /data/spark/ann, classify each pattern with an id
// create a neural net with hidden layers and train it with this data. Then 
// validate the network by re testing with the same data to check that the training 
// has worked. 

// import packages

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.mllib.classification.ANNClassifier
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd.RDD


object testann1 extends App
{

  // define data

//  val server = "file://"
//  val path   = "/home/hadoop/spark/ann/data/"

  val server = "hdfs://hc2nn.semtech-solutions.co.nz:8020"
  val path   = "/data/spark/ann/"

  val data1 = server + path + "close_square.img"
  val data2 = server + path + "close_triangle.img"
  val data3 = server + path + "lines.img"
  val data4 = server + path + "open_square.img"
  val data5 = server + path + "open_triangle.img"
  val data6 = server + path + "plus.img"

  // create the context and config

  val sparkMaster = "spark://hc2nn.semtech-solutions.co.nz:8077"
  val appName = "ANN 1"
  val conf = new SparkConf()

  conf.setMaster(sparkMaster)
  conf.setAppName(appName)

  val sparkCxt = new SparkContext(conf)

  // Load data files  into variables and split data by spaces

  val rData1 = sparkCxt.textFile(data1).map(_.split(" ").map(_.toDouble)).collect
  val rData2 = sparkCxt.textFile(data2).map(_.split(" ").map(_.toDouble)).collect
  val rData3 = sparkCxt.textFile(data3).map(_.split(" ").map(_.toDouble)).collect
  val rData4 = sparkCxt.textFile(data4).map(_.split(" ").map(_.toDouble)).collect
  val rData5 = sparkCxt.textFile(data5).map(_.split(" ").map(_.toDouble)).collect
  val rData6 = sparkCxt.textFile(data6).map(_.split(" ").map(_.toDouble)).collect

  val inputs = Array[Array[Double]] (
     rData1(0), rData2(0), rData3(0), rData4(0), rData5(0), rData6(0) )

  // create output labels

  val outputs = Array[Double]( 0.1, 0.2, 0.3, 0.4, 0.5, 0.6 )

  val ioData = inputs.zip( outputs )

  // now create a label point RDD from the raw data

  val lpData = ioData.map{ case(features,label) =>

    LabeledPoint( label, Vectors.dense(features) ) 
  }

  // need an RDD of LabeledPoint not an Array !

  val rddData = sparkCxt.parallelize( lpData )

  // set up the neural net, numbers represent node volumes in input, hidden and 
  // output layers

  val hiddenTopology : Array[Int] = Array( 100, 100 )

  val maxNumIterations = 1000
  val convTolerance    = 1e-4
  val batchSize        = 6

  // now train the neural net 

  val annModel = ANNClassifier.train(rddData,
                                     batchSize,
                                     hiddenTopology,
                                     maxNumIterations,
                                     convTolerance)

  // set up data for ANN prediction

  val rPredictData = inputs.map{ case(features) =>

    ( Vectors.dense(features) ) 
  }

  val rddPredictData = sparkCxt.parallelize( rPredictData )

  // as a test run the same inputs against the trained model 
  // to ensure the same outputs are given 

  val predictions = annModel.predict( rddPredictData )

  // force predictions to local array then print because this is running on 
  // a cluster

  predictions.toArray().foreach( value => println( "prediction > " + value ) )

} // end ann1 


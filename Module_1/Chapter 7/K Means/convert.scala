
// example script to convert text data into a numeric vector for spark naive bayes 
// algorithm. The data starts as this 
//
//   Male,Suspicion of Alcohol,Weekday,12am-4am,75,30-39
//   Male,Moving Traffic Violation,Weekday,12am-4am,0,20-24
//   Male,Suspicion of Alcohol,Weekend,4am-8am,12,40-49
//   Male,Suspicion of Alcohol,Weekday,12am-4am,0,50-59
//   Female,Road Traffic Collision,Weekend,12pm-4pm,0,20-24
//
// and ends up as this 
//
//   0,3 0 0 75 3
//   0,0 0 0 0 1
//   0,3 1 1 12 4
//   0,3 0 0 0 5
//   1,2 1 3 0 1
//
// because all of the values in the data can be enumerated. 

// import packages

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._


object convert1 extends App
{

//------------------------------------------------------------------------------------
// define a function to enumerate a CSV record

def enumerateCsvRecord( colData:Array[String]): String =
{
    // enumerate column 0 

    val colVal1 = 
      colData(0) match
      {
        case "Male"                          => 0
        case "Female"                        => 1
        case "Unknown"                       => 2
        case _                               => 99
      }

    // enumerate column 2 

    val colVal2 = 
      colData(1) match
      {
        case "Moving Traffic Violation"      => 0
        case "Other"                         => 1
        case "Road Traffic Collision"        => 2
        case "Suspicion of Alcohol"          => 3
        case _                               => 99
      }

    // enumerate column 3

    val colVal3 = 
      colData(2) match
      {
        case "Weekday"                       => 0
        case "Weekend"                       => 0
        case _                               => 99
      }

    // enumerate column 4

    val colVal4 = 
      colData(3) match
      {
        case "12am-4am"                      => 0
        case "4am-8am"                       => 1
        case "8am-12pm"                      => 2
        case "12pm-4pm"                      => 3
        case "4pm-8pm"                       => 4
        case "8pm-12pm"                      => 5
        case _                               => 99
      }

    val colVal5 = colData(4)

    val colVal6 = 
      colData(5) match
      {
        case "16-19"                         => 0
        case "20-24"                         => 1
        case "25-29"                         => 2
        case "30-39"                         => 3
        case "40-49"                         => 4
        case "50-59"                         => 5
        case "60-69"                         => 6
        case "70-98"                         => 7
        case "Other"                         => 8
        case _                               => 99
      }

    // create a string from the enumerated values

    val lineString = colVal1+","+colVal2+","+colVal3+","+colVal4+","+colVal5+","+colVal6

    return lineString
}
//-------------------------------------------------------------------------------------

  // define variables

  val hdfsServer = "hdfs://hc2nn.semtech-solutions.co.nz:8020"
  val hdfsPath   = "/data/spark/kmeans/"

  val inDataFile  = hdfsServer + hdfsPath + "DigitalBreathTestData2013-MALE2.csv"
  val outDataFile = hdfsServer + hdfsPath + "result"

  val sparkMaster = "spark://hc2nn.semtech-solutions.co.nz:7077"
  val appName = "Convert 1"
  val sparkConf = new SparkConf()

  sparkConf.setMaster(sparkMaster)
  sparkConf.setAppName(appName)

  // create the spark context

  val sparkCxt = new SparkContext(sparkConf)

  // load raw csv data from hdfs

  val csvData = sparkCxt.textFile(inDataFile)

  println("Records in  : "+ csvData.count() )

  // split cvs data by comma's to get column data array

  val enumRddData = csvData.map
  {
    csvLine =>
      val colData = csvLine.split(',')

      enumerateCsvRecord(colData)

  } // end csv data processing

  // then  write string RDD to hdfs out file

  println("Records out : "+ enumRddData.count() )

  enumRddData.saveAsTextFile(outDataFile)

} // end object



name := "A N N"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.3.0"

libraryDependencies += "org.apache.spark" % "spark-core"  % "1.3.0" from "file:///home/hadoop/spark/spark/core/target/spark-core_2.10-1.3.0-SNAPSHOT.jar"

libraryDependencies += "org.apache.spark" % "spark-mllib" % "1.3.0" from "file:///home/hadoop/spark/spark/mllib/target/spark-mllib_2.10-1.3.0-SNAPSHOT.jar"

libraryDependencies += "org.apache.spark" % "akka" % "1.3.0" from "file:///home/hadoop/spark/spark/assembly/target/scala-2.10/spark-assembly-1.3.0-SNAPSHOT-hadoop2.3.0-cdh5.1.2.jar"


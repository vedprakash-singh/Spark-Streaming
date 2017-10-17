name := "WordCount"

version := "0.1"

scalaVersion := "2.12.3"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core_2.11
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.2.0" % "provided"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming_2.11
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.2.0" % "provided"


// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka_2.10
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.6.3"
import org.apache.spark.{SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

object Spark_streaming {

  def main(args:Array[String]){
    val sc = new SparkConf().setMaster("local[2]").setAppName("WordCount")
    val ssc = new StreamingContext(sc, Seconds(2))
    val zook = "localhost:2181"
    val group = "test-wordCount"
    val topics = "count"
    val numThreads = 1
    val topicMap = topics.split(",").map((_,numThreads.toInt)).toMap
    val lineMap = KafkaUtils.createStream(ssc, zook, group,
      topicMap)
    val lines = lineMap.map(_._2)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(x => (x,1))
    val runningCounts = pairs.updateStateByKey( (values: Seq[Int],state: Option[Int]) => Some(state.sum + values.sum))
    runningCounts.print
    ssc.start
    ssc.awaitTermination


  }

}

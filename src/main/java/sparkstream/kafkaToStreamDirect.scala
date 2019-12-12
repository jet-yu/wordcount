package sparkstream

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object kafkaToStreamDirect {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local[1]")
      .setAppName("wordcountKafkaStreamingDirect")
      .set("spark.cores.max", "8")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val brokers = "192.168.40.20:9092"
    val topics = "topic_1117_streaming"
    val topicSet = topics.split(",").toSet
    val kafkaParams = Map[String,String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topicSet)

    val lines = messages.map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x=>(x,1)).reduceByKey(_+_)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()

  }
}

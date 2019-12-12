package sparkstream

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils



object kafkaToStream {

   def updateFunc(currentValues:Seq[Int],preValues:Option[Int]):Option[Int] ={
     val current = currentValues.sum
     val pre = preValues.getOrElse(0)
     Some(current+pre)
   }
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("wordcountKafkaStreaming").set("spark.cores.max", "8")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    ssc.checkpoint("hdfs://master:9000/hdfs_checkpoint")

    val zkQuorum = "master:2181,slave1:2181,slave2:2181"
    val groupId = "group_1"


    // val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)
    // val topicAndLine: ReceiverInputDStream[(String, String)] =
    //    KafkaUtils.createStream(ssc, zkQuorum, groupId, topic, StorageLevel.MEMORY_ONLY)
    // params: [ZK quorum], [consumer group id], [per-topic number of Kafka partitions to consume]

    //Map("topic_1013" -> 1） 1代表一个partition
    val topicAndLine:ReceiverInputDStream[(String,String)] =
    KafkaUtils.createStream(ssc,zkQuorum,groupId,Map("topic_1117_streaming" -> 1),StorageLevel.MEMORY_AND_DISK_SER)

    val lines:DStream[String] = topicAndLine.map{
      x=> x._2
    }

    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x=>(x,1)).updateStateByKey(updateFunc _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()

  }

}

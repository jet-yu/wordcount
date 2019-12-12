package sparkstream

import java.util

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer

object kafkaStreamKafka {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]")
      .setAppName("wordcountKafkaStreaming")
      .set("spark.cores.max", "8")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    ssc.checkpoint("hdfs://master:9000/hdfs_checkpoint")

    val zkQuorum = "master:2181,slave1:2181,slave2:2181"
    val groupId = "group_1"
    val topicAndLine: ReceiverInputDStream[(String, String)] =
      KafkaUtils.createStream(ssc, zkQuorum, groupId, Map("topic_1117_streaming" -> 1), StorageLevel.MEMORY_AND_DISK_SER)
    val lines: DStream[String] = topicAndLine.map {
      x => x._2
    }
    val array = ArrayBuffer[String]()
    lines.foreachRDD(rdd=>{
      val count = rdd.count().toInt
      rdd.take(count+1).take(count).foreach(x=>{
        array +=x+"--read"
      })
      ProducerSender(array)
      array.clear()
    })
    ssc.start()
    ssc.awaitTermination()
  }

  def ProducerSender(args:ArrayBuffer[String]):Unit={
    if(args!=null){
      val brokers = "192.168.40.20:9092"
      val props = new util.HashMap[String,Object]
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,brokers)
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
      val producer = new KafkaProducer[String,String](props)
      val topic = "topic_1117_streaming_new"
      for (arg<-args){
        println("i have send message:"+arg)
        val message = new ProducerRecord[String,String](topic,null,arg)
        producer.send(message)
      }
      Thread.sleep(500)
      producer.close()
    }
  }




}

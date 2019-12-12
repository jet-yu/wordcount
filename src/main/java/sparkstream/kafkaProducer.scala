package sparkstream

import java.util

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object kafkaProducer {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("kafkaProducer")
    val sc = new SparkContext(conf)
    val array = ArrayBuffer("111", "222", "333")
    ProducerSender(array)
  }

  def ProducerSender(args: ArrayBuffer[String]): Unit = {
    if (args != null) {
      val brokers = "192.168.40.20:9092"
      val props = new util.HashMap[String, Object]()
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,brokers)
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
     val producer = new KafkaProducer[String,String](props)
      val topic = "topic_1117_streaming"
      for(arg<-args){
        println("i have sendMessage:" + arg)
        val message = new ProducerRecord[String,String](topic,null,arg)
        producer.send(message)
      }
      Thread.sleep(500)
      producer.close()

    }
  }
}

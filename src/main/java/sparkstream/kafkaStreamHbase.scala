package sparkstream

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


object HbaseHandler {

  def insert(row: String, column: String, value: String) {
    //Hbase 配置
    val tableName = "sparkstream_kafkahbase_table"
    //定义表名
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "master,slave1,slave2")
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.set("hbase.defaults.for.version.skip", "true")
    val hTable = new HTable(hbaseConf, tableName)
    val thePut = new Put(row.getBytes())
    thePut.add("info".getBytes(), column.getBytes(), value.getBytes())
    hTable.setAutoFlush(false, false)
    //写入缓存数据
    hTable.setWriteBufferSize(3 * 1024 * 1024)
    hTable.put(thePut)
    //提交
    hTable.flushCommits()

  }
}


object kafkaStreamHbase {
  def main(args: Array[String]): Unit = {
    val zkQuorum = "master:2181;slave1:2181,slave2:2181"
    val group = "group_1"
    val topics = "topic_kafka_stream_habse"
    val numThreads = 1
    var output = "hdfs://master:9000/stream_out/spark-log.txt"

    val sparkConf = new SparkConf().setAppName("kafkaStreamHbase").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf,Seconds(10))
    ssc.checkpoint("hdfs://master:9000/hdfs_checkpoint")

    val topicMap  = topics.split(",").map((_,numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc,zkQuorum,group,topicMap).map(_._2)
    val line = lines.flatMap(_.split("\n"))
    val words = line.map(_.split("\\|"))
    words.foreachRDD(rdd=>{
        rdd.foreachPartition(partitionOfecords=>{
          partitionOfecords.foreach(pair=>{
            val key = pair(0)
            val col = pair(1)
            val value = pair(2)
            println(key+"_"+col+"_"+value)
            HbaseHandler.insert(key,col,value)
          })
        })
    })
    ssc.start()
    ssc.awaitTermination()
  }
}

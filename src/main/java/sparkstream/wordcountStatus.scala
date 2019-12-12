package sparkstream

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object wordcountStatus  {

  def updateFunction(currentValues:Seq[Int],preValues:Option[Int]):Option[Int]={
    val current = currentValues.sum
    val pre = preValues.getOrElse(0)
    Some(current+pre)
  }


  sparkstream.wordcountStatus

  def main(args: Array[String]): Unit = {
    if(args.length<2) {
      System.err.println("Usage: WordCountState <hostname><port>")
    }
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("WordcountStatus")
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    ssc.checkpoint("hdfs://master:9000/hdfs_checkpoint")

    val lines = ssc.socketTextStream(args(0),args(1).toInt,StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x=>(x,1)).updateStateByKey(updateFunction _)
    wordCounts.print()
    wordCounts.saveAsTextFiles("hdfs://master:9000/stream_stas_out","doc")
    ssc.start()
    ssc.awaitTermination()
  }

}

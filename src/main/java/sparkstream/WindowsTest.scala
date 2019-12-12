package sparkstream

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WindowsTest {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("windowTest")
    val ssc = new StreamingContext(sparkConf,Seconds(10))
    ssc.checkpoint("hdfs://master:9000/hdfs_checkpoint")

    val lines = ssc.socketTextStream(args(0),args(1).toInt,StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.flatMap(_.split(" "))
    val wordcounts = words.map(x => (x,1)).reduceByKeyAndWindow((v1:Int,v2:Int) => v1+v2,Seconds(30),Seconds(10))
    wordcounts.print()
    ssc.start()
    ssc.awaitTermination()

  }

}

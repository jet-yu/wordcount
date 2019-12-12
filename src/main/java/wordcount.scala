package spark.src.main.java

import org.apache.spark.{SparkConf, SparkContext}

object wordcount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("wordcount")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("/home/badou/badou_test/mr_fenfa/mr_count_word/new/The_Man_of_Property.txt")
    val data = rdd.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).map{x =>
      x._1+"\t"+x._2
    }
    data.saveAsTextFile("/home/badou/badou_test/spark/1.data")
  }
}

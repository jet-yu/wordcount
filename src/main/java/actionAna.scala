import org.apache.spark.{SparkConf, SparkContext}

object actionAna {

  //目标  每个用户前五喜欢的帖子
  //user watch list
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    //    conf.setMaster("local[2]")
    conf.setAppName("user watchl ist")
    val sc = new SparkContext(conf)

    val input = "hdfs://master:9000/train_new.data"
    val output = "hdfs://master:9000/user_watch_list"
    //val output = "/home/badou/badou_test/spark/2.data"

    val rdd = sc.textFile(input)
    val data = rdd.filter { x =>
      val fileds = x.split("\t")
      fileds(2).toDouble > 2

    }.map { x =>
      val fileds = x.split("\t")
      (fileds(0).toString(), fileds(1).toString(), fileds(2).toString())

    }.groupBy(_._1)
      .map{ x =>

        val userid = x._1
        val tuple_array = x._2
        val array1 = tuple_array.toArray.sortWith(_._2 > _._2)

        var num = array1.length

        if(num>5){
          num = 5
        }
        val strBulider = new StringBuilder()

        for (i <-0 until num){
          strBulider ++= array1(i)._2
          strBulider.append(":")
          strBulider ++= array1(i)._3
          strBulider.append(" ")
        }
        //        println(strBulider)
        // userid + "\t"+ strBulider.toString()
        (userid,strBulider.toString())
      }.sortBy(_._1)

    data.saveAsTextFile(output)
  }

}

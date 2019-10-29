package com.atguigu.bigdata.spark

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * 广播变量（调优策略）：共享读
  *
  * @Author cuihaiyan
  * @Create_Time 2019-10-29 07:48
  */
object Spark10_Oper1_BroadCast {
  def main(args: Array[String]): Unit = {
    //设定spark计算框架的运行环境
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WorkCount-BroadCast")
    //创建spark上下文对象
    val sc = new SparkContext(config)

    val rdd1: RDD[(Int, String)] = sc.makeRDD(List((1,"a"),(2,"b"),(3,"c")))


    //调优策略，使用广播变量可以减少数据的传输
    val list = List((1,1),(2,2),(3,3))
    //1、构建广播变量（eg：只查询的表）
    val broadcast: Broadcast[List[(Int, Int)]] = sc.broadcast(list)

    val resultRDD: RDD[(Int, (String, Any))] = rdd1.map {
      case (key, value) => {
        var v2: Any = null
        //2、使用广播变量
        for (t <- broadcast.value) {
          if (key == t._1) {
            v2 = t._2
          }
        }
        (key, (value, v2))
      }
    }
    resultRDD.foreach(println)
    /**output:
      * (2,(b,2))
      * (3,(c,3))
      * (1,(a,1))
      */


    sc.stop()
  }
}

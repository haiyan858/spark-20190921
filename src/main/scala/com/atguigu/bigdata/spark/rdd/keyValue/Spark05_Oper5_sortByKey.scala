package com.atguigu.bigdata.spark.rdd.keyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * key_value 类型的算子
  *
  * 在一个（k,v）的RDD上调用，K必须实现接口Ordered，返回一个按照K排序的（k,v）的RDD
  *
  * sortByKey: 在（K,V）中按照K值排序
  *
  * @Author cuihaiyan
  * @Create_Time 2019-09-28 21:33
  */
object Spark05_Oper5_sortByKey {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(conf)

    val rdd: RDD[(Int, String)] = sc.parallelize(Array((3,"aa"),(6,"cc"),(2,"bb"),(1,"dd")))

    //按照key的正序
    rdd.sortByKey(true).collect()
    //res26: Array[(Int, String)] = Array((1,dd), (2,bb), (3,aa), (6,cc))

    //按照key的倒序
    rdd.sortByKey(false).collect()
    //res27: Array[(Int, String)] = Array((6,cc), (3,aa), (2,bb), (1,dd))


    //关闭资源
    sc.stop()

  }
}

package com.atguigu.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * key_value 类型的算子
  *
  * 需求：创建一个pairRDD，并将Value添加字符串"|||"
  *
  * mapValues 对（K,V）中的V 进行操作
  *
  * @Author cuihaiyan
  * @Create_Time 2019-09-28 21:47
  */
object Spark05_Oper6_mapValues {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(conf)

    val rdd: RDD[(Int, String)] = sc.parallelize(Array((11,"aa"),(22,"bb"),(33,"cc")))

    val result: RDD[(Int, String)] = rdd.mapValues(_+"111")

    result.foreach(println)
    /*
    output:
    (11,aa111)
    (22,bb111)
    (33,cc111)


    scala> rdd.mapValues(_+"|||").collect
    res28: Array[(Int, String)] = Array((11,aa|||), (22,bb|||), (33,cc|||))
    */

  }
}

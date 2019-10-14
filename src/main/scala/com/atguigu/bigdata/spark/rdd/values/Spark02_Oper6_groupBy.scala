package com.atguigu.bigdata.spark.rdd.values

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  *
  *
  * @Author cuihaiyan
  * @Create_Time 2019-09-26 19:28
  */
object Spark02_Oper6_groupBy {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("wordCount")
    val sc = new SparkContext(conf)

    val listRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))

    //按照指定规则分组
    //Each group consists of a key and a sequence of elements mapping to that key.
    val groupByRDD: RDD[(Int, Iterable[Int])] = listRDD.groupBy(i => i % 2)

    groupByRDD.collect().foreach(println)
    /*
    output:
    (0,CompactBuffer(2, 4, 6, 8, 10))
    (1,CompactBuffer(1, 3, 5, 7, 9))
    */

  }
}

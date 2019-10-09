package com.atguigu.bigdata.spark.rdd.keyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * key-value类型的算子
  *
  * 创建一个pairRDD，根据key计算每种key的均值
  * 先计算每个key出现的次数，再计算对应key的总和，再相除得到平均值
  *
  * combineByKey: 计算均值
  *
  * @Author cuihaiyan
  * @Create_Time 2019-09-28 20:39
  */
object Spark05_Oper4_combineByKey {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(conf)

    val input: RDD[(String, Int)] = sc.parallelize(List(("a",88),("b",95),("a",91),("b",93),("a",95),("b",98)),2)
    val combine: RDD[(String, (Int, Int))] = input.combineByKey(
      (_, 1), //转换数据结构 ("a",88) =》 "a" (88，1)
      (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1), //分区内计算 (88，1) => ((88,1),91)=>  ((88+91),2)
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    )
    //res19: Array[(String, (Int, Int))] = Array((b,(286,3)), (a,(274,3)))

    combine.foreach(println)
    /*
    output:
    (a,(274,3))
    (b,(286,3))
    */

    val result: RDD[(String, Double)] = combine.map {
      case (key, value) => (key, value._1 / value._2.toDouble)
    }
    //res23: Array[(String, Double)] = Array((b,95.33333333333333), (a,91.33333333333333))

    result.foreach(println)
    /*
    output:
    (b,95.33333333333333)
    (a,91.33333333333333)
    */


    //停止：关闭资源
    sc.stop()

  }
}

package com.atguigu.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * k-v 类型的算子
  * cogroup：在类型为（K,V）和（K,W）的RDD上调用，返回一个新的RDD，类型为：（K,Iterable(V),Iterable(W)）
  * key 相同的，可为空
  *
  * @Author cuihaiyan
  * @Create_Time 2019-09-28 23:17
  */
object Spark05_Oper8_cogroup {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(conf)

    val rdd1: RDD[(Int, String)] = sc.parallelize(Array((1, "a"), (2, "b"), (3, "c")))
    val rdd2: RDD[(Int, Int)] = sc.parallelize(Array((1, 4), (2, 5), (3, 6)))

    rdd1.cogroup(rdd2).collect().foreach(println)
    /*
    output:
    (1,(CompactBuffer(a),CompactBuffer(4)))
    (2,(CompactBuffer(b),CompactBuffer(5)))
    (3,(CompactBuffer(c),CompactBuffer(6)))
     */

    /*
    scala> rdd1.cogroup(rdd2).collect
    res31: Array[(Int, (Iterable[String], Iterable[Int]))] =
    Array(
    (1,(CompactBuffer(a),CompactBuffer(4))),
    (2,(CompactBuffer(b),CompactBuffer(5))),
    (3,(CompactBuffer(c),CompactBuffer(6)))
    )
    */
  }
}

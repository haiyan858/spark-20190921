package com.atguigu.bigdata.spark.rdd.keyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * key-value类型的算子
  *
  * foldByKey: 分区内和分区间的计算函数一样
  *
  * @Author cuihaiyan
  * @Create_Time 2019-09-28 10:09
  */
object Spark05_Oper3_foldByKey {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.parallelize(List(("a",3),("a",2),("c",4),("b",3),("c",6),("c",8)),2)
    //res12: Array[Array[(String, Int)]] = Array(Array((a,3), (a,2), (c,4)), Array((b,3), (c,6), (c,8)))

    //计算相同key的相加结果：

    //分区中key第一次出现时，zerovalue的值设置为0
    //分区内\分区间相同key求和
    val foldByKey: RDD[(String, Int)] = rdd.foldByKey(0)(_+_)
    //res15: Array[(String, Int)] = Array((b,3), (a,5), (c,18))

    foldByKey.collect.foreach(println)
    /*
    output:
    ((b,3)
    (a,5)
    (c,18)
    */


    /*
    cala> var rdd = sc.parallelize(List(("a",3),("a",2),("c",4),("b",3),("c",6),("c",8)),2)
    rdd: org.apache.spark.rdd.RDD[(String, Int)] = ParallelCollectionRDD[11] at parallelize at <console>:24

    scala> rdd.glom.collect
    res12: Array[Array[(String, Int)]] = Array(Array((a,3), (a,2), (c,4)), Array((b,3), (c,6), (c,8)))

    scala> rdd.foldByKey(0)(_+_).collect
    res15: Array[(String, Int)] = Array((b,3), (a,5), (c,18))
    */

    //停止：关闭资源
    sc.stop()

  }
}

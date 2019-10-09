package com.atguigu.bigdata.spark.rdd.keyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * key-value类型的算子
  *
  * reduceByKey: 在一个（k,v）的RDD上调用，返回一个（k,v）的RDD，使用指定的reduce函数，
  * 将相同的key值聚合到一起，reduce任务的个数可以通过第二个可选的参数来设置
  *
  * @Author cuihaiyan
  * @Create_Time 2019-09-28 10:09
  */
object Spark05_Oper1_reduceByKey {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(conf)

    //创建一个pairRDD
    val words = Array("one", "two", "two", "three", "three", "three")

    val wordPairsRDD: RDD[(String, Int)] = sc.parallelize(words).map(word => (word, 1))

    //将相同key对应值聚合到一个sequence中
    val group: RDD[(String, Int)] = wordPairsRDD.reduceByKey(_+_)

    group.collect.foreach(println)
    /*
    output:
    (two,2)
    (one,1)
    (three,3)
    */

    //停止：关闭资源
    sc.stop()

  }
}

package com.atguigu.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
  * key-value类型的算子
  *
  * groupByKey: 对每个key操作，但只生成一个sequence
  *
  *
  * @Author cuihaiyan
  * @Create_Time 2019-09-27 23:19
  */
object Spark04_Oper7_groupByKey {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(conf)

    //创建一个pairRDD
    val words = Array("one","two","two","three","three","three")

    val wordPairsRDD: RDD[(String, Int)] = sc.parallelize(words).map(word => (word,1))

    //将相同key对应值聚合到一个sequence中
    val group: RDD[(String, Iterable[Int])] = wordPairsRDD.groupByKey()

    group.collect.foreach(println)
    /*
    output:
    (two,CompactBuffer(1, 1))
    (one,CompactBuffer(1))
    (three,CompactBuffer(1, 1, 1))
    */

    //计算相同key对应值的相加结果
    val resultRDD: RDD[(String, Int)] = group.map(t => (t._1, t._2.sum))
    resultRDD.collect.foreach(println)

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
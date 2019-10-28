package com.atguigu.bigdata.spark

import java.util

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}

/**
  * 自定义累加器
  *
  * @Author cuihaiyan
  * @Create_Time 2019-10-28 22:47
  */
object Spark09_Oper3_ShareData {

  def main(args: Array[String]): Unit = {
    //设定spark计算框架的运行环境
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WorkCount")
    //创建spark上下文对象
    val sc = new SparkContext(config)

    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    // 创建累加器对象
    val accumulator: LongAccumulator = sc.longAccumulator


    sc.stop()
  }
}

//声明累加器
//1、继承AccumulatorV2
//2、实现抽象方法
//3、创建累加器
class WordAccumulator extends AccumulatorV2[String,util.ArrayList[String]] {
  private val list = new util.ArrayList[String]()

  //初始化zero：当前的累加器是否为初始化状态
  override def isZero: Boolean = list.isEmpty

  //复制累加器对象
  override def copy(): AccumulatorV2[String, util.ArrayList[String]] = {
    new WordAccumulator()
  }

  //重置累加器对象
  override def reset(): Unit = {
    list.clear()
  }

  //向累加器中增加数据
  override def add(v: String): Unit = {
    if (v.contains("h")){
      list.add(v)
    }
  }

  //合并两个累加器对象
  override def merge(other: AccumulatorV2[String, util.ArrayList[String]]): Unit = {
    list.addAll(other.value)
  }

  override def value: util.ArrayList[String] = ???
}









package com.atguigu.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * distinct
  * 将rdd中一个分区的数据打乱重组到其他不同的分区的操作， 称之为 shuffle 操作
  * 总结：spark 中的所有转换算子中没有shuffle操作，性能比较快
  *
  * @Author cuihaiyan
  * @Create_Time 2019-09-27 14:19
  */
object Spark03_Oper9_distinct {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sparkContext = new SparkContext(conf)

    val listRDD: RDD[Int] = sparkContext.makeRDD(List(1, 1, 2, 2, 4, 5, 5, 6))

    //val distinctRDD: RDD[Int] = listRDD.distinct()
    // 使用distinct算子去重后，数据变少， 可以改变分区的数量
    // 把结果放到2个分区中
    val distinctRDD: RDD[Int] = listRDD.distinct(2)

    //distinctRDD.collect().foreach(println)
    distinctRDD.saveAsTextFile("output-distinct")

  }
}

package com.atguigu.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * key-value类型的算子
  *
  * SaggregateByKey: 分区内，分区间
  *
  * @Author cuihaiyan
  * @Create_Time 2019-09-28 10:09
  */
object Spark05_Oper2_aggregateByKey {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.parallelize(List(("a",3),("a",2),("c",4),("b",3),("c",6),("c",8)),2)
    //res12: Array[Array[(String, Int)]] = Array(Array((a,3), (a,2), (c,4)), Array((b,3), (c,6), (c,8)))

    //分区中key第一次出现时，zerovalue的值设置为0
    //分区内相同key取最大值
    //分区间相同key求和
    val aggRDD: RDD[(String, Int)] = rdd.aggregateByKey(0)(math.max(_,_),_+_)

    //分区中key第一次出现时，zerovalue的值设置为0
    //分区内相同key求和
    //分区间相同key求和
    //rdd.aggregateByKey(0)(_+_,_+_)

    aggRDD.collect.foreach(println)
    /*
    output:
    ((b,3)
    (a,3)
    (c,12)
    */


    /*s
    cala> var rdd = sc.parallelize(List(("a",3),("a",2),("c",4),("b",3),("c",6),("c",8)),2)
    rdd: org.apache.spark.rdd.RDD[(String, Int)] = ParallelCollectionRDD[11] at parallelize at <console>:24

    scala> rdd.glom.collect
    res12: Array[Array[(String, Int)]] = Array(Array((a,3), (a,2), (c,4)), Array((b,3), (c,6), (c,8)))

    scala> var agg = rdd.aggregateByKey(0)(math.max(_,_),_+_)
    agg: org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[13] at aggregateByKey at <console>:25

    scala> agg.collect
    res13: Array[(String, Int)] = Array((b,3), (a,3), (c,12))

    scala> var agg = rdd.aggregateByKey(10)(math.max(_,_),_+_)
    agg: org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[14] at aggregateByKey at <console>:25

    scala> agg.collect
    res14: Array[(String, Int)] = Array((b,10), (a,10), (c,20))
    */
    //停止：关闭资源
    sc.stop()

  }
}
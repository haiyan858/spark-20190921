package com.atguigu.bigdata.spark.rdd.keyValue

/**
  * k-v 类型算子
  * join：两个k-v类型的，根据相同的key，做join
  * 如果是多个k-v的pairRDD，结果是tuple 套着tuple
  *
  * @Author cuihaiyan
  * @Create_Time 2019-09-28 23:09
  */
object Spark05_Oper7_join {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(conf)

    val rdd1: RDD[(Int, String)] = sc.parallelize(Array((1, "a"), (2, "b")))
    val rdd2: RDD[(Int, String)] = sc.parallelize(Array((1, "aa"), (2, "bb")))

    rdd1.join(rdd2).collect().foreach(println)
    /*
    output:
    (1,(a,aa))
    (2,(b,bb))
    */

    /*
    scala> rdd.join(rdd2).collect
    res29: Array[(Int, (String, String))] = Array((1,(a,aa)), (2,(b,bb)))
    */

  }
}

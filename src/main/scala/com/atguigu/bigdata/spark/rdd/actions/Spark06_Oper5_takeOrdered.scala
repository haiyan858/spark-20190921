package com.atguigu.bigdata.spark.rdd.actions

/**
  * @Author cuihaiyan
  * @Create_Time 2019-10-09 11:23
  */
object Spark06_Oper5_takeOrdered {

  //takeOrdered(n)
  //rdd排序后，返回rdd中前n个元素，数组的形式返回

  /**
    * scala> val rdd = sc.parallelize(Array(2,5,4,6,8,3))
    * rdd: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[5] at parallelize at <console>:24
    * *
    * scala> rdd.take
    * take   takeAsync   takeOrdered   takeSample
    * *
    * scala> rdd.takeOrdered(3)
    * res11: Array[Int] = Array(2, 3, 4)
    *
    */
}

package com.atguigu.bigdata.spark.rdd.actions

/**
  * @Author cuihaiyan
  * @Create_Time 2019-10-09 11:23
  */
object Spark06_Oper4_take {

  //take(n)
  //返回rdd中前n个元素，数组的形式返回

  /**
    * scala> val rdd = sc.parallelize(1 to 10)
    * rdd: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[4] at parallelize at <console>:24
    *
    * scala> rdd.count()
    * res6: Long = 10
    *
    * scala> rdd.first()
    * res8: Int = 1
    *
    * scala> rdd.take(3)
    * res9: Array[Int] = Array(1, 2, 3)
    *
    * scala> rdd.take(5)
    * res10: Array[Int] = Array(1, 2, 3, 4, 5)
    *
    */
}

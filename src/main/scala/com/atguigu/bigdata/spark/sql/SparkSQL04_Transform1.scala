package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * RDD 直接转为 DataSet
  *
  * @Author cuihaiyan
  * @Create_Time 2019-11-07 00:11
  */
object SparkSQL04_Transform1 {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL03_Transform")

    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    //RDD 与 DF、DS 转换，需要引入隐式转换
    //spark => SparkSession 的对象名字
    import spark.implicits._

    //创建RDD
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1,"zhangSann",25),(2,"list",34),(3,"wangWu",35)))

    val userRDD: RDD[User] = rdd.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }

    val userDS: Dataset[User] = userRDD.toDS()

    val rdd1: RDD[User] = userDS.rdd

    rdd1.foreach(println)
    /*
    User(2,list,34)
    User(1,zhangSann,25)
    User(3,wangWu,35)
    */


    //释放资源
    spark.stop()
  }
}

//样例类
//case class User(id:Int, name: String, age:Int)

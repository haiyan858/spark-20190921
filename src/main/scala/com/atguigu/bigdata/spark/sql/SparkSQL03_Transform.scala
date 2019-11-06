package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * RDD、DF、DS 转换
  *
  * @Author cuihaiyan
  * @Create_Time 2019-11-06 23:53
  */
object SparkSQL03_Transform {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL03_Transform")

    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    //RDD 与 DF/DS 转换，需要引入隐式转换
    //spark => SparkSession 的对象名字
    import spark.implicits._

    //创建RDD
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1,"zhangSann",25),(2,"list",34),(3,"wangWu",35)))

    //转为DF
    val df: DataFrame = rdd.toDF("id","name","age")

    //转为DS
    val ds: Dataset[User] = df.as[User]

    //转为DF
    val df1: DataFrame = ds.toDF()

    //转为RDD
    val rdd1: RDD[Row] = df1.rdd

    rdd1.foreach(row => {
      println(row.getInt(0),row.getString(1),row.getInt(2))
    })


    //释放资源
    spark.stop()
  }
}

//样例类
case class User(id:Int, name: String, age:Int)
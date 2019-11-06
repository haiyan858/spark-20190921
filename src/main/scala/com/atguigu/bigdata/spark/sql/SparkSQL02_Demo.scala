package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * DataFrame 转为一张表
  *
  * @Author cuihaiyan
  * @Create_Time 2019-11-06 23:49
  */
object SparkSQL02_Demo {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")

    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    // Display the content of the DataFrame to stdout
    val df: DataFrame = spark.read.json("data/people.json")
    df.show()
    /*
    +----+-------+
    | age|   name|
    +----+-------+
    |null|Michael|
    |  30|   Andy|
    |  19| Justin|
    +----+-------+
    */

    //DataFrame 转为一张表
    df.createOrReplaceTempView("user")

    spark.sql("select * from user").show()

    //释放资源
    spark.stop()
  }

}

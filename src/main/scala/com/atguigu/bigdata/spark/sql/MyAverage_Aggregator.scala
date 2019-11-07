package com.atguigu.bigdata.spark.sql

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator

/**
  * 求平均工资
  *
  * 强类型用户自定义聚合函数：
  * 通过继承Aggregator来实现强类型自定义聚合函数
  *
  * @Author cuihaiyan
  * @Create_Time 2019-11-07 23:22
  */

// 既然是强类型，可能有case类
case class Employee(name: String, salary: Long)
case class Average(var sum: Long, var count: Long)

//Aggregator[-IN, BUF, OUT]
object MyAverage_Aggregator extends Aggregator[Employee,Average,Double] {

  // 定义一个数据结构，保存工资总数和工资总个数，初始都为0
  override def zero: Average = Average(0L,0L)

  // Combine two values to produce a new value. For performance, the function may modify `buffer`
  // and return it instead of constructing a new object
  override def reduce(buffer: Average, employee: Employee): Average = {
    buffer.sum += employee.salary
    buffer.count += 1
    buffer
  }

  // 聚合不同execute的结果
  override def merge(b1: Average, b2: Average): Average = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }

  // 计算输出
  override def finish(reduction: Average): Double = {
    reduction.sum.toDouble/reduction.count
  }

  // 设定之间值类型的编码器，要转换成case类
  // Encoders.product是进行scala元组和case类转换的编码器
  override def bufferEncoder: Encoder[Average] = Encoders.product

  // 设定最终输出值的编码器
  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble

}

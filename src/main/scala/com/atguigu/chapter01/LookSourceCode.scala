package com.atguigu.chapter01

/**
  *
  *
  * @Author cuihaiyan
  * @Create_Time 2019-09-22 10:38
  */
object LookSourceCode {
  /**
    * 主函数
    *
    * @param args 行参
    */
  def main(args: Array[String]): Unit = {

    val v42 = 42
    3 match {
      case `v42` => println("42")
      case _     => println("Not 42")
    }

    val UppercaseVal = 42
    3 match {
      case UppercaseVal => println("42")
      case _            => println("Not 42")
    }
  }
}

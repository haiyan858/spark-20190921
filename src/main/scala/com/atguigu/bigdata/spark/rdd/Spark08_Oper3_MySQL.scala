package com.atguigu.bigdata.spark.rdd

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  *
  * @Author cuihaiyan
  * @Create_Time 2019-10-16 23:40
  */
object Spark08_Oper3_MySQL {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(conf)

    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/rdd"
    val user = "root"
    val password = "123456"

    /*
    // 查询数据
    val sql = "select name,age from user where id >= ? and id <= ?"
    //创建jdbcRDD
    val jdbcRDD = new JdbcRDD(
      sc,
      () => {
        //获取数据库连接对象
        Class.forName(driver)
        DriverManager.getConnection(url, user, password)

      },
      sql,
      1,
      3,
      2,
      (rs)=>{
        println(rs.getString(1)+", "+rs.getInt(2))
      }
    )

    jdbcRDD.collect()
    */

    //保存数据
    val dataRDD: RDD[(String, Int)] = sc.makeRDD(List(("zs", 20), ("ls", 30), ("ww", 40)))

    /*
    Class.forName(driver)
    val connection: Connection = DriverManager.getConnection(url,user,password)

    dataRDD.foreach{
      case(name,age) => {
        val sql = "insert into user(name,age) values(?,?)"

        val statement: PreparedStatement = connection.prepareStatement(sql)
        statement.setString(1,name)
        statement.setInt(2,age)
        statement.executeUpdate()

        statement.close()
      }
    }
    connection.close()
    */

    dataRDD.foreachPartition(datas => {
      Class.forName(driver)
      val connection: Connection = DriverManager.getConnection(url, user, password)

      datas.foreach {
        case (name, age) => {
          val sql = "insert into user(name,age) values(?,?)"

          val statement: PreparedStatement = connection.prepareStatement(sql)
          statement.setString(1, name)
          statement.setInt(2, age)
          statement.executeUpdate()

          statement.close()
        }
      }
      connection.close()
    })


    //释放资源
    sc.stop()

  }
}

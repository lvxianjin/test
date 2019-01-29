package com.hive

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lxj
  * @Date: 2018/11/1 15:29
  * @Version 1.0
  */
object HiveTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()
    sqlContext.sql("desc hivetest.student").show()  //能获取到表的结构，获取不到表的数据
  }
}

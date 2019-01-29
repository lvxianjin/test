package com.hdfs

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: lxj
  * @Date: 2018/10/30 10:44
  * @Version 1.0
  */
object inputTest {
  def main(args: Array[String]): Unit = {
    println("Test Start")
    val sparkConf = new SparkConf().setAppName("HDFSTest").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val data=sc.textFile("hdfs://192.168.17.135:9000/SparkTest/test.txt")
    data.collect().foreach{println}
    data.repartition(1).saveAsTextFile("hdfs://192.168.17.135:9000/SparkTest")
    //data.saveAsTextFile("hdfs://192.168.17.135:9000/SparkTest/test.txt")
  }
}

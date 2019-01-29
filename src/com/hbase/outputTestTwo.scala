package com.hbase
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark._
/**
  * @Author: lxj
  * @Date: 2018/10/29 16:20
  * @Version 1.0
  */
object outputTestTwo {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("HBaseTest")
    val sc = new SparkContext(sparkConf)
    val tablename = "hbase1"
    sc.hadoopConfiguration.set("hbase.zookeeper.quorum","slave1,slave2")
    sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort", "2181")
    sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, tablename)
    val job = new Job(sc.hadoopConfiguration)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    val indataRDD = sc.makeRDD(Array("lv4,value4","lv5,value5"))
    val rdd = indataRDD.map(_.split(',')).map{arr=>{
      val put = new Put(Bytes.toBytes(arr(0).toString))
      put.add(Bytes.toBytes("f1"),Bytes.toBytes("test"),Bytes.toBytes(arr(1)))
      (new ImmutableBytesWritable, put)
    }}
    rdd.saveAsNewAPIHadoopDataset(job.getConfiguration())
  }
}

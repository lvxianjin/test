package com.hbase
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
/**
  * @Author: lxj
  * @Date: 2018/10/29 14:01
  * @Version 1.0
  */
object outputTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("HBaseTest")
    val sc = new SparkContext(sparkConf)
    val conf = HBaseConfiguration.create()
    //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
    conf.set("hbase.zookeeper.quorum","slave1,slave2")
    //设置zookeeper连接端口，默认2181
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    val tablename = "hbase1"
    //初始化jobconf，TableOutputFormat必须是org.apache.hadoop.hbase.mapred包下的！
    val jobConf = new JobConf(conf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tablename)
    val indataRDD = sc.makeRDD(Array("lv2,value1","lv3,value3"))
    val rdd = indataRDD.map(_.split(',')).map{arr=>{
         /*一个Put对象就是一行记录，在构造方法中指定主键
37        * 所有插入的数据必须用org.apache.hadoop.hbase.util.Bytes.toBytes方法转换
38        * Put.add方法接收三个参数：列族，列名，数据
39        */
          val put = new Put(Bytes.toBytes(arr(0).toString))  //通过行键构造put对象
          put.add(Bytes.toBytes("f1"),Bytes.toBytes("test"),Bytes.toBytes(arr(1)))
          //转化成RDD[(ImmutableBytesWritable,Put)]类型才能调用saveAsHadoopDataset
          (new ImmutableBytesWritable, put)
           }}
    rdd.saveAsHadoopDataset(jobConf)
    sc.stop()
  }
}

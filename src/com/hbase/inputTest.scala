package com.hbase
/**
  * @Author: lxj
  * @Date: 2018/10/26 16:37
  * @Version 1.0
  */
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
object inputTest {
  def main(args: Array[String]): Unit = {
    val conf = HBaseConfiguration.create();  //建立Hbase的连接
    //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
    conf.set("hbase.zookeeper.quorum","slave1,slave2")
    //设置zookeeper连接端口，默认2181
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set(TableInputFormat.INPUT_TABLE,"hbase1") //设置查询的表名hbase1
    val sc = new SparkContext(new SparkConf().setAppName("hbasetest").setMaster("local"));
    val testRdd = sc.newAPIHadoopRDD(conf,classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result]);//通过SparkContext将student表中数据创建一个rdd
    testRdd.cache();//持久化
    val count = testRdd.count(); //计算数据条数
    println("test rdd count:"+count);
    //遍历输出
    //当我们建立Rdd的时候，前边全部是参数信息，后边的result才是保存数据的数据集
    testRdd.foreach({case (_,result) =>
      //通过result.getRow来获取行键
      val key = Bytes.toString(result.getRow);
      //通过result.getValue("列族"，"列名")来获取值
      //注意这里需要使用getBytes将字符流转化字节流
      val value = Bytes.toString(result.getValue("f1".getBytes,"test".getBytes));
      println("Row key:"+key+" hf1:"+value);
    });
  }
}

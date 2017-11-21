package com.vesper.test

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 找出数据中第三列的数据中的最大值
  */
object ThreeMax {
  def main(args: Array[String]): Unit = {
    //配置conf属性，设置应用名并指定master节点的端口号以及本地jar文件的存放位置
    val conf = new SparkConf().setAppName("ThreeMax").setMaster("spark://192.168.19.139:7077")
      .setJars(Seq("C:\\Users\\vespe\\IdeaProjects\\untitled3\\out\\artifacts\\untitled3_jar\\untitled3.jar"))
    //创建sparkContext
    val scontext = new SparkContext(conf)
    //在HDFS上读取文件，并通过map分割后那到第三个数据段，
    // 之后用reduce进行判断返回最大值
    val lines = scontext.textFile("hdfs://192.168.19.139:9000//data//ThreeMax.log")
      .map(_.split(Array(',',' ','\t'))(2).toInt).reduce((a, b) => if(a>b) a else b)
    println(lines)
  }

}

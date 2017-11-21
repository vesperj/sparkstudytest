package com.vesper.test

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 求出数据中的最大值和最小值
  * 在集群环境中运行
  */
object MaxNum {
  def main(args: Array[String]): Unit = {
    //设置conf属性，必须指定名字和master节点端口以及本地jar包的位置
    val conf = new SparkConf().setAppName("MaxNum").setMaster("spark://192.168.19.139:7077")
      .setJars(Seq("C:\\Users\\vespe\\IdeaProjects\\sparkstudytest\\out\\artifacts\\sparkstudytest_jar\\sparkstudytest.jar"))
    //创建sparkContext
    val sc = new SparkContext(conf)
    //从hdfs上面读取数据源，转变成tuple，
    // RDD —map—》tuple（后面的参数是一个Int的最大值） —reduce—》比较两组元组中的第一个数字，分为四种情况进行划分
    val lines = sc.textFile("hdfs://192.168.19.139:9000//data//MaxOrMin.log").map(x => (x.toInt, Integer.MAX_VALUE))
      .reduce((x, y) => if (x._1 > y._1 && x._2 > y._1) (x._1, y._1) else if (x._1 < y._1 && x._1 > x._2) (y._1, x._2)
      else if (x._1 < y._1 && x._1 < x._2) (y._1, x._1) else (x._1, x._2))
    println(lines)
    sc.stop()
  }


}

package com.vesper.test

import org.apache.spark.{SparkConf, SparkContext}

object MaxNum {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MaxNum").setMaster("spark://192.168.19.139:7077")
      .setJars(Seq("C:\\Users\\vespe\\IdeaProjects\\sparkstudytest\\out\\artifacts\\sparkstudytest_jar\\sparkstudytest.jar"))
    val sc = new SparkContext(conf)
    val lines = sc.textFile("hdfs://192.168.19.139:9000//data//MaxOrMin.log")
    val num = lines.map(_.toInt)
    val max = num.reduce((a, b) => if (a > b) a else b)
    val min = num.reduce((a, b) => if (a < b) a else b)
    println(s"max = $max , min = $min")
    sc.stop()
  }


}

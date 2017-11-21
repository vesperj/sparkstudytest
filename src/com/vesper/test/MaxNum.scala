package com.vesper.test

import org.apache.spark.{SparkConf, SparkContext}

object MaxNum {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MaxNum").setMaster("spark://192.168.19.139:7077")
      .setJars(Seq("C:\\Users\\vespe\\IdeaProjects\\sparkstudytest\\out\\artifacts\\sparkstudytest_jar\\sparkstudytest.jar"))
    val sc = new SparkContext(conf)
    val lines = sc.textFile("hdfs://192.168.19.139:9000//data//MaxOrMin.log").map(x => (x.toInt, Integer.MAX_VALUE))
      .reduce((x, y) => if (x._1 > y._1 && x._2 > y._1) (x._1, y._1) else if (x._1 < y._1 && x._1 > x._2) (y._1, x._2)
      else if (x._1 < y._1 && x._1 < x._2) (y._1, x._1) else (x._1, x._2))
    println(lines)
    sc.stop()
  }


}

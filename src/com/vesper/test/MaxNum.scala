package com.vesper.test

import org.apache.spark.{SparkConf, SparkContext}

object MaxNum {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MaxNum").setMaster("spark://192.168.19.139:7077")
      .setJars(Seq("C:\\Users\\vespe\\IdeaProjects\\sparkstudytest\\out\\artifacts\\sparkstudytest_jar\\sparkstudytest.jar"))
    val sc = new SparkContext(conf)
//    val lines = sc.textFile("hdfs://192.168.19.139:9000//data//MaxOrMin.log")
//    lines.map(_.split("\\s+")).reduce((a, b) => if (a > b) a else b)
    //    val num = lines.map(_.toInt).cache()
    //    val max = num.reduce((a,b) => if (a > b) a else b)
    //    val min = num.reduce((a, b) => if (a < b) a else b)
    //    lines.map(_.toInt).reduce((a, b) => if (a > b) a else b)

    val lines = sc.textFile("hdfs://192.168.19.139:9000//data//MaxOrMin.log").map(_.toInt).reduce((a, b) => if (a > b) a else b)

//    val max = sc.textFile("hdfs://192.168.19.139:9000//data//MaxOrMin.log")
//      .map(line => line.split(" ").size).reduce((a, b) => if (a > b) a else b)
//    println(s"max = $max , min = $min")
    sc.stop()
  }
//  def fi(a,b) = {
//    println(s"a=$a , b=$b")
//    if(a<b) a else b
//  }

}

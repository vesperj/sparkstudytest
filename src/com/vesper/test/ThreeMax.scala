package com.vesper.test

import org.apache.spark.{SparkConf, SparkContext}

object ThreeMax {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ThreeMax").setMaster("spark://192.168.19.139:7077")
      .setJars(Seq("C:\\Users\\vespe\\IdeaProjects\\untitled3\\out\\artifacts\\untitled3_jar\\untitled3.jar"))
    val scontext = new SparkContext(conf)
    val lines = scontext.textFile("hdfs://192.168.19.139:9000//data//ThreeMax.log")
      .map(_.split(Array(',',' ','\t'))(2).toInt).reduce((a, b) => if(a>b) a else b)
    println(lines)
  }

}

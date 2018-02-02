package com.bigdata.spark.wordcount

import org.apache.spark.{SparkConf, SparkContext}
object wordcount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wordcount")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("hdfs://centos01:9000/杂记.txt.copy",1)

    val words = lines.flatMap { lines => lines.split(" ")}
    val pairs = words.map {words => (words,1)}
    val counts = pairs.reduceByKey { _ + _ }
    counts.foreach(word => println("word:" + word._1 + "times:" + word._2))
  }

}

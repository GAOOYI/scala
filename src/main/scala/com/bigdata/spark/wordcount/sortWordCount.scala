package com.bigdata.spark.wordcount

import org.apache.spark.{SparkConf, SparkContext}

object sortWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sortWordCount").setMaster("local").set("spark.testing.memory","471859200")
    val sc = new SparkContext(conf)

    sc.textFile("H:\\sparkJar\\spark.txt")
      .flatMap(line => line.split(" "))
        .map(word => (word,1))
      .reduceByKey(_ + _)
      .map(sortedWord => (sortedWord._2, sortedWord._1))
      .sortByKey(false)
      .foreach(t => println("word:" + t._2 + "++" + t._1 + "times"))
  }

}

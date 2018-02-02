package com.bigdata.spark.sorted

import org.apache.spark.{SparkConf, SparkContext}

object secondSorted {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("secondSorted").set("spark.testing.memory","471859200")
    val sc = new SparkContext(conf)

    sc.textFile("H:\\sparkJar\\numbers.txt")
      .map(line => (new key(line.split(" ")(0).toInt,line.split(" ")(1).toInt),line))
        .sortByKey(false)
      .foreach(t => println(t._2))

  }

}

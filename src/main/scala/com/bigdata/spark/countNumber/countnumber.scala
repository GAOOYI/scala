package com.bigdata.spark.countNumber

import org.apache.spark.{SparkConf, SparkContext}

object countnumber {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("countNumber").setMaster("local").set("spark.testing.memory","471859200")

    val sc = new SparkContext(conf)

    val lines = sc.textFile("H:\\sparkJar\\spark.txt")

    val count = lines.map { line => line.trim.length }.reduce { _ + _ }

    println(count)

  }

}

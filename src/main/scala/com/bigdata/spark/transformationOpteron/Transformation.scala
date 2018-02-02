package com.bigdata.spark.transformationOpteron

import org.apache.spark.{SparkConf, SparkContext}

object Transformation {
  def main(args: Array[String]): Unit = {
    //
    // map
    //filter
    groupByKey
  }

  def map: Unit = {
    val conf = new SparkConf().setAppName("map").setMaster("local").set("spark.testing.memory","471859200")
    val sc = new SparkContext(conf)

    val list = Array(1,2,3,4,5,6,7)
    val num = sc.parallelize(list)
    val mutiple = num.map {
      _ * 2
    }
    mutiple.foreach(i => println(i))
  }

  def filter: Unit ={
    val conf = new SparkConf().setAppName("filter").setMaster("local").set("spark.testing.memory","471859200")
    val sc = new SparkContext(conf)

    val list = Array(1,2,3,4,5,6,7)
    val num = sc.parallelize(list)
    val even = num.filter(i => i % 2 == 0)
    even.foreach(i => println(i))
  }

  def groupByKey: Unit ={
    val conf = new SparkConf().setAppName("filter").setMaster("local").set("spark.testing.memory","471859200")
    val sc = new SparkContext(conf)

    val scoreList = Array(Tuple2("class1",90),Tuple2("class2",80),Tuple2("class1",65),Tuple2("class2",70))
    val scores = sc.parallelize(scoreList)
    val group = scores.groupByKey()
    group.foreach(s => {println(s._1); s._2.foreach(i => print(i + " "));println()})

  }

}

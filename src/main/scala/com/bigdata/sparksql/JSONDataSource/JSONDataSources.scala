package com.bigdata.sparksql.JSONDataSource

import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

object JSONDataSources {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("JSONDataSources").set("spark.testing.memory","471859200")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //利用json文件创建dataset
    val studentScoresDF = sqlContext.read.json("hdfs://centos01:9000/spark-study/students.json");

    //将创建的dataset注册临时表，查询
    studentScoresDF.createOrReplaceTempView("student_scores");
    val GT80student = sqlContext.sql("select name,score from student_scores where score >= 80")

    val GT80studentName = GT80student.rdd.map(row => row(0)).collect()

    //针对JavaRDD<String>创建DataFrame
    val studentInfoJSONS = Array("{\"name\":\"Leo\",\"age\":\"18\"}",
      "{\"name\":\"Marry\",\"age\":\"19\"}",
      "{\"name\":\"Jack\",\"age\":\"17\"}")

    val studentInfoRDD = sc.parallelize(studentInfoJSONS)
    //针对这个RDD，将其转换为Dataset
    val studentInfoDs = sqlContext.read.json(studentInfoRDD)

    ////针对这个Dataset注册临时表，查询
    studentInfoDs.createOrReplaceTempView("student_info")
    var sql = "select name,age from student_info where name in ("
    for (i <- 0 until  GT80studentName.length){
      sql  += "'" + GT80studentName(i) + "'"
      if (i < GT80studentName.size - 1) sql += ","
    }
    sql += ")"

    val GT80studentInfoDs = sqlContext.sql(sql)
    val DTds = GT80student.rdd.map(row => (row.getAs[String]("name"), row.getAs[Long]("score")))
    //执行join操作
    val JoinRow = GT80studentInfoDs.rdd.map(row => (row.getAs[String]("name"),row.getAs[String]("age")))
      .join(DTds)
      .map(t => Row(t._1,t._2._1.toInt,t._2._2.toInt))

    val structFields = Array(StructField("name",StringType,true),StructField("age",IntegerType,true),StructField("score",IntegerType,true))

    val structType = StructType(structFields)

    val ds = sqlContext.createDataFrame(JoinRow,structType)

    //ds.write.format("json").mode(SaveMode.Append).save("hdfs://centos01:9000/spark-study/studentsInfo.json")
    ds.show()

  }

}

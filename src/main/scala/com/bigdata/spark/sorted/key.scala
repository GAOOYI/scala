package com.bigdata.spark.sorted

class key(val first:Int, val second:Int) extends Ordered[key] with Serializable {
  override def compare(that: key):Int = {
    if (this.first - that.first != 0){
      this.first - that.first
    }else{
      this.second - that.second
    }
  }
}

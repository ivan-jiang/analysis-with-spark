package com.ivan.spark.ch3

object Demo {
  def main(args: Array[String]): Unit = {
    val data = Array(1, 2, 3)
    val dd = (data: _*)
    println(dd)
  }
}

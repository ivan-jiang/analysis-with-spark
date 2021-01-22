package com.ivan.spark.ch4

object Demo {
  def main(args: Array[String]): Unit = {
    val list1 = List(1, 2, 3)
    val list2 = list1.map(_ + 1)
    println(list2)
  }
}

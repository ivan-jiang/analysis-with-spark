package com.ivan.scala

object Demo {
  def main(args: Array[String]): Unit = {
    val list1 = Seq(1, 2, 3)
    val list2 = list1.map(_ + 1)
    println(list2)

    val set1 = Set(1, 2, 3, 2, 1, 5)
  }
}

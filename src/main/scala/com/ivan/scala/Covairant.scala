package com.ivan.scala

/**
 * 协变 & 逆变
 */
object Covairant {
  def main(args: Array[String]): Unit = {
    val c1: Consumer[Level2, Level2] = new Consumer[Level1, Level3]
    println(c1.m1())
    c1.m2(new Level3)
  }
}

class Level1 {}

class Level2 extends Level1 {}

class Level3 extends Level2 {}

class Level4 extends Level3 {}

class Consumer[-IN, +OUT] {
  def m1[U >: OUT](): U = new OUT // 协变，下界
  def m2[U <: IN](u: U) = println(u) // 逆变，上界
}

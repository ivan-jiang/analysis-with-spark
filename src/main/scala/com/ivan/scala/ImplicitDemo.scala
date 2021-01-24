package com.ivan.scala

object ImplicitContext {
  implicit val sex: String = "man"
}

object ImplicitContextMutil {
  implicit val sex: String = "woman"
  implicit val he: String = "he"
}

object ImplicitParam {
  def implicitMethod(name: String)(implicit aaa: String): Unit = {
    println(name + " is good " + aaa)
  }

  def main(args: Array[String]): Unit = {
    import ImplicitContextMutil.sex
    implicitMethod("Ivan")
  }
}

package com.ivan.spark.ch4

object Demo {
  def main(args: Array[String]): Unit = {
    val list = List(1, 2, 3)
    // :: 用于的是向队列的头部追加数据,产生新的列表, x::list,x就会添加到list的头部
    println(4 :: list) //输出: List(4, 1, 2, 3),等价于list.::(4)
    // .:: 这个是list的一个方法;作用和上面的一样,把元素添加到头部位置; list.::(x);
    println(list.::(5)) //输出: List(5, 1, 2, 3)
    // :+ 用于在list尾部追加元素; list :+ x;
    println(list :+ 6) //输出: List(1, 2, 3, 6)
    // +: 用于在list的头部添加元素;
    val list2 = "A" +: "B" +: Nil //Nil Nil是一个空的List,定义为List[Nothing]
    println(list2) //输出: List(A, B)
    // ::: 用于连接两个List类型的集合 list ::: list2
    println(list ::: list2) //输出: List(1, 2, 3, A, B)
    // ++ 用于连接两个集合，list ++ list2
    println(list ++ list2) //输出: List(1, 2, 3, A, B)

    val seq1 = Seq(1, 2, 3)
    val seq2 = Seq(4, 5)
    println(seq1 ++ seq2)

    val list3 = "A" :: "B" :: Nil
    println(list3)

    val ls = List(3, -1)
    ls match {
      case 0 +: Nil => println("only 0")
      case x +: y +: Nil => println(s"hello x : $x  y: $y ")
      case 0 +: tail => println("0 ....")
      case _ => println("others")
    }
  }
}

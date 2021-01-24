package com.ivan.scala

import scala.collection.IndexedSeqLike
import scala.collection.generic.CanBuildFrom
import scala.collection.mutable.{ArrayBuffer, Builder}

abstract class Base

case object A extends Base

case object B extends Base

case object C extends Base

case object D extends Base

// ref https://scala.cool/2017/07/a-new-collection/
object Base {
  // fromInt是一个函数，函数定义Int=>Base,函数内容：Array(A, B, C, D)
  val fromInt: Int => Base = {
    Array(A, B, C, D)
  }
  val toInt: Base => Int = {
    Map(A -> 0, B -> 1, C -> 2, D -> 3)
  }
}

final class Message private(val groups: Array[Int], val length: Int)
  extends IndexedSeq[Base] with IndexedSeqLike[Base, Message] {

  import Message._

  override protected[this] def newBuilder: Builder[Base, Message] = Message.newBuilder

  override def apply(idx: Int): Base = {
    if (idx < 0 || length <= idx) {
      throw new IndexOutOfBoundsException
    }
    Base.fromInt(groups(idx / N) >> (idx % N * S) & M)
  }
}

object Message {
  // 表示一组所需要的位数
  private val S = 2
  // 一个Int能够放入的组数
  private val N = Integer.SIZE / S
  // 分离组的位掩码(bitmask)
  private val M = (1 << S) - 1

  def main(args: Array[String]): Unit = {
    val msg = Message(A, B, C, D)
    println(msg.length)
    println(msg.last)
    println(msg)
    println(msg.take(3))
  }

  def fromSeq(buf: Seq[Base]): Message = {
    val groups = new Array[Int]((buf.length + N - 1) / N)
    for (i <- 0 until buf.length) {
      groups(i / N) |= Base.toInt(buf(i)) << (i % N * S)
    }
    new Message(groups, buf.length)
  }

  def apply(bases: Base*): Message = fromSeq(bases)

  def newBuilder: Builder[Base, Message] =
    new ArrayBuffer[Base]().mapResult(fromSeq)

  implicit def canBuildFrom: CanBuildFrom[Message, Base, Message] = {
    new CanBuildFrom[Message, Base, Message] {
      override def apply(from: Message): Builder[Base, Message] = newBuilder

      override def apply(): Builder[Base, Message] = newBuilder
    }
  }
}

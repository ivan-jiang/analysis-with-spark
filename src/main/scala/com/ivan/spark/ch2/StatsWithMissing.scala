package com.ivan.spark.ch2

import org.apache.spark.rdd.RDD
import org.apache.spark.util.StatCounter

class NAStatCounter extends Serializable {
  val stats: StatCounter = new StatCounter()
  var missing: Long = 0

  def add(x: Double) = {
    if (java.lang.Double.isNaN(x)) {
      // NaN
      missing += 1
    } else {
      stats.merge(x)
    }

    this
  }

  def merge(other: NAStatCounter) = {
    stats.merge(other.stats)
    missing += other.missing
    this
  }

  override def toString: String = {
    "stats:" + stats.toString() + ",missing:" + missing
  }
}

object NAStatCounter {
  def apply(x: Double): NAStatCounter = new NAStatCounter().add(x)

  def statsWithMissing(rdd: RDD[Array[Double]]): Array[NAStatCounter] = {
    // 为每个分区的每个score创建1个NAStatCounter
    val counterRDD: RDD[Array[NAStatCounter]] = rdd.map(array => array.map(x => NAStatCounter(x)))
    counterRDD.reduce((x, y) => x.zip(y).map { case (a, b) => a.merge(b) })
  }

  def statsWithMissingV2(rdd: RDD[Array[Double]]): Array[NAStatCounter] = {
    val counterRDD: RDD[Array[NAStatCounter]] = rdd.mapPartitions(iter => {
      // 每个分区的数据只需要创建一组（9个）NAStatCounter,然后用这组NAStatCounter（9个）去merge其它数据
      val counters = iter.next().map(x => NAStatCounter(x))
      while (iter.hasNext) {
        val array = iter.next()
        (0 until counters.length).foreach(i => counters(i).add(array(i)))
      }
      Iterator(counters)
    })
    counterRDD.reduce((x, y) => {
      x.zip(y).map { case (a, b) => a.merge(b) }
    })
  }
}
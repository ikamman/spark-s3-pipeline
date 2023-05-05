package com.spark.home.assignment

import org.apache.spark.rdd.RDD

object Processing {

  val seqOp = (_: Int, it: Iterable[KeyValue]) => it.size
  val combOp = (fst: Int, snd: Int) => fst + snd

  def findMaxOddValueCountForKey(
      input: RDD[KeyValue]
  ): RDD[KeyValue] = input
    .groupBy(identity)
    .aggregateByKey(0)(seqOp, combOp)
    .filter(_._2 % 2 != 0)
    .map(_._1)
}

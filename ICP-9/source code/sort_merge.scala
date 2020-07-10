package mergesort

import org.apache.spark.{SparkConf, SparkContext}

object sort_merge{
  def merge(xs: List[Int], ys: List[Int]): List[Int] =
    (xs, ys) match {
      case (Nil, ys) => ys
      case (xs, Nil) => xs
      case (x :: xs1, y :: ys1) =>
        if (x < y) x :: merge(xs1, ys)
        else y :: merge(xs, ys1)
    }

  def mergeSort(xs: List[Int]): List[Int] = {
    val n = xs.length / 2
    if (n == 0) xs
    else {
      val (left, right) = xs splitAt (n)
      merge(mergeSort(left), mergeSort(right))
    }
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("MergeSort App").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val list = List(55, 2, 30, 100,65, 78,47)
    val result = sc.parallelize(Seq(list)).map(mergeSort)
    result.foreach(println)
  }
}
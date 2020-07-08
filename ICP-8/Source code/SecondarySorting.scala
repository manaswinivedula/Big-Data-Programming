import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object SecondarySorting {

  // user defined function to parse the input file in to required format
  def parsing_line(line: String) = {
    val fields = line.split(",")
    val year = fields(0).toInt
    val month = fields(1).toInt
    val day = fields(2).toInt
    val value= fields(3).toInt
    val k = (year + "-" + month)
    val k2 = (k, value)
    (k2, value)
  }
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("SecondarySorting")
    val sc = new SparkContext(conf)
    // Loading the input file.
    val lines = sc.textFile("C:\\Users\\manas\\IdeaProjects\\ICP8\\input2.txt")
    // Split up lines into key and value pairs
    val rdd = lines.map(parsing_line)
    val partitionedRDD = rdd.partitionBy(new HashPartitioner(1))
  //grouping them with the dates and sorting them according to the values in the list
    val listRDD = partitionedRDD.map(l => (l._1._1, l._2)).groupByKey().mapValues(k => k.toList.sortBy(r => r))
    listRDD.collect().foreach(println)
        //saving into output file
    listRDD.saveAsTextFile("secondarySorting_output")
  }


}
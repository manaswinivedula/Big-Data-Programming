import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object bonus {
  case class Person(age: Long, gender: String, country: String, state: String)
  def parseLine(line: String): Person = {
    val fields = line.split(",")
    val person: Person = Person(fields(1).toLong, fields(2), fields(3), fields(4))
    return person
  }
  def main(args: Array[String]): Unit = {
    //    Set the log level only to print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    //    Create SparkSession
    val sparkSession = SparkSession.builder()
      .appName("Spark SQL Bonus-Question")
      .master("local[*]").getOrCreate()
    //  Create SQLContext
    //    val SQLContext = sparkSession.sqlContext
    val lines = sparkSession.sparkContext.textFile("survey.csv")
    //  To skip the header
    val lines2 = lines.mapPartitionsWithIndex {
      (idx, iter) => if (idx == 0) iter.drop(1) else iter
    }
    val Survey=lines2.map(parseLine)
    import sparkSession.implicits._
    val df = Survey.toDF()
    import org.apache.spark.ml.feature.OneHotEncoder
    import org.apache.spark.ml.feature.StringIndexer
    val sample = df.filter(df("age")>=13 and df("age")<=20).select("age" , "gender","country", "state").show(10)
    //
    val sampleIndexedDf = new StringIndexer().setInputCol("gender").setOutputCol("gender_label").fit(df).transform(df)

    sampleIndexedDf.show()
    val correlation= sampleIndexedDf.stat.corr("age","gender_label")
    println("correlation is", correlation)
    val covariance=sampleIndexedDf.stat.cov( "age", "gender_label")
    println("covariance is", covariance)

  }

}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame
import org.apache.spark.sql.functions._

object Task1 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    System.setProperty("hadoop.home.dir","C:\\winutils")
    val spark = SparkSession.builder().appName("GraphFrameApplication").master("local[*]").getOrCreate()

    // 1.	Import the dataset as a csv file and create data frames directly on import than create graph out of the data frame created.
    val station_df = spark.read.format("csv").option("header", true).load("201508_station_data.csv")
    println("1. creating data frames directly on import than create graph out of the data frame created.")
    station_df.show(10)

    val trip_df = spark.read.format("csv").option("header", true).load("201508_trip_data.csv")
    trip_df.show(10)


    //  2.	Concatenate chunks into list & convert to Data Frame
    println("2. Concatenate chunks into list & converting them  into Data Frame")
    station_df.select(concat(col("lat"), lit(","), col("long")).as("lat_long")).show(10, false);

    //  3.	Remove duplicates
    println("3. removing duplicates")
    val stationDF = station_df.dropDuplicates()
    val tripDF = trip_df.dropDuplicates()

    //    4.	Name Columns
    println("4.Naming columns")
    val renamed_tripDF = tripDF.withColumnRenamed("Trip ID", "tripId")
      .withColumnRenamed("Start Date", "StartDate")
      .withColumnRenamed("Start Station", "StartStation")
      .withColumnRenamed("Start Terminal", "src")
      .withColumnRenamed("End Date", "EndDate")
      .withColumnRenamed("End Station", "EndStation")
      .withColumnRenamed("End Terminal", "dst")
      .withColumnRenamed("Bike #", "BikeNum")
      .withColumnRenamed("Subscriber Type", "SubscriberType")
      .withColumnRenamed("Zip Code", "ZipCode")

    //    5.	Output Data Frame
    println("5.Output Data frame")
    stationDF.show(10, false)
    renamed_tripDF.show(10, false)


    //    6.	Create vertices
    println("6. creating vertices")
    val vertices = stationDF.select(col("station_id").as("id"),
      col("name"),
      concat(col("lat"), lit(","), col("long")).as("lat_long"),
      col("dockcount"),
      col("landmark"),
      col("installation"))


    val edges = renamed_tripDF.select("src", "dst", "tripId", "StartDate", "StartStation", "EndDate", "EndStation", "BikeNum", "SubscriberType", "ZipCode")
    vertices.show(10, false)


    val g = GraphFrame(vertices, edges)


    //    7.	Show some vertices
    println("7. showing some vertices")
    g.vertices.select("*").orderBy("landmark").show()


    //    8.	Show some edges
    println("8. showing some edges")
    g.edges.groupBy("src", "StartStation", "dst", "EndStation").count().orderBy(desc("count")).show(10)


    //    9.	Vertex in-Degree
    println("9. Vertex in-degree")
    val in_Degree = g.inDegrees
    in_Degree.orderBy(desc("inDegree")).show(8, false)


    //    10.	Vertex out-Degree
    println("10. vertex out-Degree")
    val out_Degree = g.outDegrees
    out_Degree.show(10)
    vertices.join(out_Degree, Seq("id")).show(10)


    //    11.	Apply the motif findings.
    println("11. Apply the motif findings")
    val motifs = g.find("(a)-[ab]->(b); (b)-[bc]->(c); (c)-[ca]->(a)").show(10, false)


    //    Bonus
    //    1.Vertex degree
    println("1. degree of vertexes")
    g.degrees.show(10)

    //    2. what are the most common destinations in the dataset from location to location.
    println("2. what are the most common destinations in the dataset from location to location.")
    g.edges.groupBy("src", "dst").count().orderBy(desc("count")).show(10)

    //    3. what is the station with the highest ratio of in degrees but fewest out degrees. As in, what station acts as almost a pure trip sink. A station where trips end at but rarely start from.
    println(("what is the station with the highest ratio of in degrees but fewest out degrees."))
    val df1 = in_Degree.orderBy(desc("inDegree"))
    val df2 = out_Degree.orderBy("outDegree")
    val df = df1.join(df2, Seq("id"))
      .selectExpr("id", "double(inDegree)/double(outDegree) as degreeRatio")
    df.orderBy(desc("degreeRatio")).limit(10).show(5, false)



    //    4.Save graphs generated to a file.
    println("4.Save graphs generated to a file")

    g.vertices.write.mode("overwrite").parquet("output_vertices")
    g.edges.write.mode("overwrite").parquet("output_edges")



    spark.stop()
  }

}
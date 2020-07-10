import org.apache.spark.{SparkConf, SparkContext}

object depth_first_search {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Depthfirstsearch").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    type Vertex = Int
    type Graph = Map[Vertex, List[Vertex]]
    val g: Graph = Map(1 -> List(2,4,6), 2 -> List(1,3,5), 3 -> List(5,6), 4 -> List(1,4),5 -> List(6),6 -> List(1,3,4))

    def DFS(start: Vertex, g: Graph): List[Vertex] = {
      def DFS0(vertex: Vertex, visited: List[Vertex]): List[Vertex] = {
        if (visited.contains(vertex)) {
          visited
        }
        else {
          val newNeighbor = g(vertex).filterNot(visited.contains)
          newNeighbor.foldLeft(vertex :: visited)((b, a) => DFS0(a, b))
        }
      }

      DFS0(start, List()).reverse
    }

    val dfsresult1 = DFS(1, g)
    println("DFS output at 1 : "+ dfsresult1.mkString(","))
    val dfsresult2 = DFS(2, g)
    println("DFS Output at 2 :" + dfsresult2.mkString(","))
    val dfsresult3 = DFS(3, g)
    println("DFS Output  at 3 :"+ dfsresult3.mkString(","))
    val dfsresult4 = DFS(4, g)
    println("DFS Output at 4 :"+ dfsresult4.mkString(","))
    val dfsresult5 = DFS(5, g)
    println("DFS Output at 5 : "+ dfsresult5.mkString(","))
    val dfsresult6 = DFS(6, g)
    println("DFS Output at 6: "+ dfsresult6.mkString(","))
  }

}
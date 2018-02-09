import java.io.{BufferedWriter, File, FileWriter}

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import scala.collection.mutable.{ListBuffer, _}
import ml.sparkling.graph.operators.OperatorsDSL._
import java.io._

object SparklingGraph {
  var hashTable = HashMap[Long, List[Long]]()

  def readTrainData(fileName: String): (String, String) = {
    val rowData = fileName.split(",").map(_.trim)
    val row = (rowData {0}, rowData {1})
    return row
  }

  def findEdge(pair: ListBuffer[Long]): Boolean = {
    var p = (pair(0), pair(1))
    var movies1 = hashTable(p._1)
    var movies2 = hashTable(p._2)
    if (movies1.intersect(movies2).size >= 3) {
      return true
    }
    return false
  }

  def generateEdges(usersGropued: RDD[(Long, Long)], sc: SparkContext): (RDD[VertexId], RDD[(VertexId, VertexId)]) = {
    var usersAll = ListBuffer[Long]()
    val userMoviesList = usersGropued.groupByKey().sortByKey().map(r => (r._1, r._2.toList))
    val userMovies = userMoviesList.collect()
    for (user <- userMovies) {
      usersAll += user._1
      hashTable += (user._1 -> user._2)
    }
    val userCombs = usersAll.combinations(2)
    val edges = sc.parallelize(userCombs.toSeq)
    val edgeRDD1 = edges.filter(r => findEdge(r)).map(r => (r(0), r(1)))
    val edgeRDD2 = edgeRDD1.map(r => r.swap)
    val edgeRDD = edgeRDD1.union(edgeRDD2)
    val vertexRDD = sc.parallelize(usersAll)
    return (vertexRDD, edgeRDD)
  }

  def main(args: Array[String]) = {

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    if (args.length < 2) {
      println("Please pass the input: <input_file> <output_file>")
      System.exit(0)
    }

    val sparkConf = new SparkConf().setAppName("movies").setMaster("local[*]")
    sparkConf.set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(sparkConf)
    val train_data_csv = sc.textFile(args(0)).map(line => readTrainData(line))
    val first2 = train_data_csv.first()
    val usersGrouped = train_data_csv.filter(row => row != first2).map(line => (line._1.toLong, line._2.toLong))
    val results = generateEdges(usersGrouped, sc)
    val userNodes = results._1.map(r => (r, 1.0))
    val graphNodes = VertexRDD(userNodes)
    val userEdges = results._2.map(r => Edge(r._1, r._2, 0.0))
    var graphG = Graph(graphNodes, userEdges)
    val c = graphG.PSCAN(epsilon = 0.75)
    var components =c.vertices.map(_.swap).groupByKey().sortByKey().map(_._2).collect()
    val f = new File(args(1))
    val dir = new File(f.getParent())
    if (!dir.exists()) dir.mkdirs()
    val f2 = new FileWriter(f.getAbsoluteFile)
    val f3 = new BufferedWriter(f2)
    println("#Communities: " + components.size)
    for (comp <- components) {
      val q = comp.toList.sorted
      val s = "[" + q.mkString(",")+"]"
      println(s)
      f3.write(s+"\n")
    }
    f3.close()
  }
}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import scala.collection.mutable.{ListBuffer, _}
import java.io._

object Betweenness {
  var hashTable = HashMap[Int,List[Int]]()
  def readTrainData(fileName: String): (String, String) = {
    val rowData = fileName.split(",").map(_.trim)
    val row = (rowData{0},rowData{1})
    return  row
  }

  def findEdge(pair: List[Int]): Boolean = {
    val p = (pair(0),pair(1))
    val movies1 = hashTable(p._1)
    val movies2 = hashTable(p._2)
    if (movies1.intersect(movies2).size >=3) return true
    return false
  }

  def generateEdges(usersGropued: RDD[(Int,Int)], sc: SparkContext): (RDD[(Int,Int)],List[List[Int]]) = {
    var usersAll = ListBuffer[Int]()
    val userMoviesList = usersGropued.groupByKey().sortByKey().map(r => (r._1,r._2.toList))
    val userMovies = userMoviesList.collect().toList
    for (user <- userMovies) {
      usersAll += user._1
      hashTable += (user._1 -> user._2)
    }
    val userCombs = usersAll.toList.combinations(2).toSeq
    val edges = sc.parallelize(userCombs)
    val edgeRDD1 = edges.filter(r => findEdge(r)).map(r => (r(0),r(1)))
    val edgeRDD2 = edgeRDD1.map(r => r.swap)
    val edgeRDD = edgeRDD1.union(edgeRDD2)
    val vertexRDD = sc.parallelize(usersAll)
    return (edgeRDD,userCombs.toList)
  }

  def main(args: Array[String]) = {

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    if( args.length < 2)
    {
      println("Please pass the input: <input_file> <output_file>")
      System.exit(0)
    }

    val sparkConf = new SparkConf().setAppName("movies").setMaster("local[*]")
    sparkConf.set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(sparkConf)
    val train_data_csv = sc.textFile(args(0)).map(line => readTrainData(line))
    val first2 = train_data_csv.first()
    val usersGrouped = train_data_csv.filter(row =>  row != first2).map(line => (line._1.toInt, line._2.toInt)).cache()
    val results = generateEdges(usersGrouped,sc)
    val vertices = results._1.groupByKey().collect()
    var userHashMap = HashMap[Int, Set[Int]]()
    for (v <- vertices) {
      var nodes = Set[Int]()
      for(u <- v._2.toList ) nodes += u
      userHashMap += (v._1 -> nodes)
    }
    var hashVertices = HashMap[Int,Double]()
    var hashSrcDst = HashMap[Int,HashMap[Int,Double]]()
    for (k <- userHashMap.keys) {
      hashVertices += (k -> 1.0)
      val hashDst = HashMap[Int, Double]()
      for (v <- userHashMap(k)) {
        hashDst += (v -> 0.0)
      }
      hashSrcDst += (k -> hashDst)
    }
    var parents = HashMap[Int,Set[Int]]()
    for (u <- hashVertices.keys){parents += (u -> Set())}
    var c = 1
    for (u1 <- hashVertices.keys){
      c+=1
      var leafMap = HashMap[Int,Set[Int]]()
      var nextSet = Set[Int]()
      var currSet = Set[Int]()
      var prevSet = Set[Int]()
      hashVertices = hashVertices.map(v => (v._1,1.0))
      nextSet.add(u1)
      parents.foreach(f => f._2.clear())
      var cnt = 0

      while (nextSet.nonEmpty) {
        for (elem <- nextSet) currSet.add(elem)
        nextSet.clear()
        var leafSet = Set[Int]()
        var visitedSet = Set[Int]()
        while (currSet.nonEmpty) {
          val src = currSet.head
          visitedSet.add(src)
          val childrenSet = userHashMap(src).toSet.diff(visitedSet.union(currSet).union(prevSet))
          if (childrenSet.size == 0) leafSet.add(src)
          else
            for (v <- childrenSet) {
              nextSet.add(v)
              parents(v).add(src)
            }
          currSet.remove(src)
        }
        prevSet.clear()
        for (elem <- visitedSet) prevSet.add(elem)
        if (leafSet.nonEmpty) leafMap += (cnt -> leafSet)
        cnt += 1
      }
      var revCnt = cnt - 1
      for (elem <- prevSet) nextSet.add(elem)
      var firstTime = true
      while (nextSet.nonEmpty) {
        if (firstTime == false && (leafMap isDefinedAt revCnt)){
          val leafSet = leafMap(revCnt)
          for (elem <- leafSet) nextSet.add(elem)
        }
        for (elem <- nextSet) currSet.add(elem)
        nextSet.clear()
        while (currSet.nonEmpty){
          val src = currSet.head
          val nodeParents = parents(src)
          val num_edges = nodeParents.size.toDouble
          if (num_edges > 0){
            for (item <- nodeParents) nextSet.add(item)
            val curr_val = hashVertices(src)
            val between = curr_val / num_edges
            val hashDst = hashSrcDst(src)
            for (elem <- nodeParents) {
              hashVertices(elem) += between
              hashDst(elem) += between
            }
            hashSrcDst(src) = hashDst
          }
          currSet.remove(src)
        }
        firstTime = false
        revCnt -= 1
      }
    }
    val hashEdges = HashMap[(Int,Int),Double]()
    for (k <- hashSrcDst.keys){
      for (v <- hashSrcDst(k).keys) {
        if (k < v) hashEdges += ((k,v) -> hashSrcDst(k)(v))
      }
    }
    val f = new  File(args(1))
    val dir = new File(f.getParent())
    if (!dir.exists()) dir.mkdirs()
    val f2 = new FileWriter(f.getAbsoluteFile)
    val f3 = new BufferedWriter(f2)
    val printList = hashEdges.map(r => (r._1._1,r._1._2,r._2)).toList.sorted
    for (e <- printList) {
      f3.write(e.toString()+"\n")
    }
    f3.close()
    print("Output written to the file")
  }
}
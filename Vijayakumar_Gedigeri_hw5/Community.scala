import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import scala.collection.mutable.{ListBuffer, _}
import java.io._

object Community {
  var hashTable = HashMap[Int,List[Int]]()
  var userHashMap = HashMap[Int, Set[Int]]()
  var degreeMap = HashMap[Int, Double]()
  var totalEdges = 0.0

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

  def doBFS(edge: (Int,Int)): Boolean = {
    var nextSet = Set[Int]()
    var visitedSet = Set[Int]()
    nextSet.add(edge._1)
    while (nextSet.nonEmpty) {
      val currList = nextSet.toList
      nextSet.clear()
      for (src <- currList){
        visitedSet.add(src)
        val children = userHashMap(src)
        if (children.contains(edge._2)) return true
        val childrenSet= children.toSet.diff(visitedSet)
        for (v <- childrenSet) nextSet.add(v)
      }
    }
    return  false
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
    //val edgeRDD = sc.parallelize(List((1,2),(1,3),(2,3),(2,4),(4,5),(4,6),(4,7),(5,6),(6,7)))
    //val edgeRDD = sc.parallelize(List((1,2),(1,3),(2,3),(3,7),(4,5),(4,6),(5,6),(6,7),(7,8),(8,9),(9,10),(10,11),(9,11),(8,12),(12,13),(12,14),(13,14)))
    //val vertices = edgeRDD.map(_.swap).union(edgeRDD).groupByKey().sortByKey().collect()
    for (v <- vertices) {
      var nodes = Set[Int]()
      for(u <- v._2.toList ) nodes += u
      userHashMap += (v._1 -> nodes)
    }
    val res = userHashMap.map(r => (r._1,r._2.size.toDouble))
    degreeMap = res
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
    println("Betweenness calculation done")
    val hashEdges = HashMap[(Int,Int),Double]()
    for (k <- hashSrcDst.keys){
      for (v <- hashSrcDst(k).keys) {
        if (k < v) hashEdges += ((k,v) -> hashSrcDst(k)(v))
      }
    }
    totalEdges = 2 * hashEdges.size
    var edgeBetween = HashMap[Double,Set[(Int,Int)]]()
    val betweenness = hashEdges.values.toSet.toList.sorted(Ordering[Double].reverse)
    for (b <- betweenness) edgeBetween += (b -> Set())
    for (edge <- hashEdges.keys){
      val value = hashEdges(edge)
      var edgeList = edgeBetween(value)
      edgeList.add(edge)
      edgeBetween(value) = edgeList
    }
    val nodes = userHashMap.keys.toArray.sorted
    var count = 0
    var allMods = ListBuffer[Double]()
    var componentsList = HashMap[Int, ListBuffer[List[Int]]]()
    var key = 0
    var sum2 = 0.0
    val combs = nodes.combinations(2).toSeq
    for (pair <- combs) {
      val kA = degreeMap(pair(0))
      val kB = degreeMap(pair(1))
      if (userHashMap(pair(0)).contains(pair(1)))
        sum2 += 1.0 - (kA * kB / totalEdges)
      else
        sum2 -= (kA * kB / totalEdges)
    }
    val originalMod = sum2 * 2 / totalEdges
    print("Calculating modularity: ")
    while (count < betweenness.size) {
      val removededEdges = edgeBetween(betweenness(count))
      if(count % 10000 == 0) print(".")
      for (removedEdge <- removededEdges) {
        var nosplit = false
        var newList = userHashMap(removedEdge._1)
        newList.remove(removedEdge._2)
        userHashMap(removedEdge._1) = newList
        newList = userHashMap(removedEdge._2)
        newList.remove(removedEdge._1)
        userHashMap(removedEdge._2) = newList
        nosplit = doBFS(removedEdge)
        if (!nosplit) {
          var visitedNodes = Array.fill(userHashMap.size)(false)
          var itercomps = ListBuffer[List[Int]]()
          var components = 0
          var sum = 0.0
          while (visitedNodes.contains(false)) {
            val root = nodes(visitedNodes.indexOf(false))
            var nextSet = Set[Int]()
            var visitedSet = Set[Int]()
            nextSet.add(root)
            while (nextSet.nonEmpty) {
              val currList = nextSet.toList
              nextSet.clear()
              for (src <- currList) {
                visitedSet.add(src)
                val childrenSet = userHashMap(src).toSet.diff(visitedSet)
                for (v <- childrenSet) nextSet.add(v)
              }
            }
            val rootNodes = visitedSet.toList.sorted
            itercomps += rootNodes
            val combs = rootNodes.combinations(2).toSeq
            for (pair <- combs) {
              val kA = degreeMap(pair(0))
              val kB = degreeMap(pair(1))
              if (userHashMap(pair(0)).contains(pair(1)))
                sum += 1.0 - (kA * kB / totalEdges)
              else
                sum -= (kA * kB / totalEdges)

            }
            for (node <- rootNodes) visitedNodes(nodes.indexOf(node)) = true
            components += 1
          }
          val mod = 2 * sum / totalEdges
          allMods += mod
          componentsList += (key -> itercomps)

        }
        else {allMods += 0.0}
        key += 1
    }
      count +=1
    }
    val max = allMods.toList.max

    val f = new  File(args(1))
    val dir = new File(f.getParent())
    if (!dir.exists()) dir.mkdirs()
    val f2 = new FileWriter(f.getAbsoluteFile)
    val f3 = new BufferedWriter(f2)
    if (max >= originalMod) {
      val index = allMods.lastIndexOf(max)
      println("#Communities: " +componentsList(index).size)
      for (comp <- componentsList(index)) {
        val s = "["+comp.mkString(",")+"]"
        println(s)
        f3.write(s+"\n")
      }
    }
    else {
      println("\n#Communities: 1")
      val s = "["+nodes.mkString(",")+"]"
      println(s)
      f3.write(s)
    }
     f3.close()
    println("\nOutput written to file")
  }
}
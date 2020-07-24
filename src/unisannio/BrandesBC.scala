package unisannio

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.util.Random
import java.io.PrintWriter
import java.io.FileWriter
import java.io.File
import scala.collection.mutable.Stack
import scala.collection.mutable.Queue
import scala.collection.mutable.PriorityQueue
import scala.math.Ordering.Implicits._
import org.apache.spark.SparkConf
import org.apache.log4j._
import org.apache.spark.graphx._
import scala.collection.immutable.ListMap
import org.apache.spark.broadcast.Broadcast

object BrandesBC {
  BasicConfigurator.configure();
  val logger = Logger.getLogger("Brandes")
  logger.setLevel(Level.INFO)
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)
  Logger.getLogger("io.netty").setLevel(Level.ERROR)
  val VERSION = "exact"

  def main(args: Array[String]) {
    if (args.length < 6) {
      System.err.print("Usage: BrandesBC <master> <graph_filename> <edge_delimiter> ")
      System.err.println("<is_directed> <is_weighted> [<is_weight_distance>] <output_folder>")
      System.exit(1)
    }
    logger.setLevel(Level.INFO)
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val conf = new SparkConf()
    /*.setMaster(args(0))
      .setAppName("BrandesBasedBetwenness")
      .set("spark.network.timeout", "100000000")
      .set("spark.driver.maxResultSize", "50g")
      .set("spark.executor.heartbeatinterval", "100000000")
      .set("spark.driver.heartbeatinverval", "100000000")
      .set("spark.driver.memory", "250g")
      .set("spark.executor.memory", "250g")*/
    val sc = new SparkContext(conf)

    //val parallelism = args(6).toInt
    val edgeFile = args(1)
    val edgedelimiter = args(2)
    val output_folder = args(6)
    var is_weight_distance = true //Optional parameter: possible values should be sim, rank and dist
    if(args.length == 7){
       is_weight_distance = args(5).equals("dist") //other value should be sim
       logger.info("There are 7 args. is_weight_distance has value " + is_weight_distance)
    }
    
    logger.info(s"Executing Brandes (Exact BC) with spark configuration " + args(0) +
      ", delimiter: \"" + edgedelimiter + "\"" +
      ", on file: " + edgeFile + ", graph is: " + args(3)
      + ", weights are distance: " + is_weight_distance +
      " and " + args(4) + " from file " + edgeFile)
    logger.info("Results will be written to " + output_folder)
    var is_directed = args(3).equals("d")
    var is_weighted = args(4).equals("w")

    val startTime = System.currentTimeMillis

    //reading dataset
    val inputHashFunc = (id: String) => id.toLong
    var edgeRDD = sc.textFile(edgeFile).map(row => {
      val tokens = row.split(edgedelimiter).map(_.trim())
      tokens.length match {
        case 2 => { new Edge(inputHashFunc(tokens(0)), inputHashFunc(tokens(1)), 1.0) }
        case 3 => { new Edge(inputHashFunc(tokens(0)), inputHashFunc(tokens(1)), tokens(2).toDouble) }
        case _ => { throw new IllegalArgumentException("invalid input line: " + row) }
      }
    })

    var max_weight = 1.0
    if (!is_weight_distance) {
      max_weight = edgeRDD.map(_.attr).max
      edgeRDD = edgeRDD.map(e => Edge(e.srcId, e.dstId, -e.attr + 1 + max_weight))
      logger.info("Weights have been inverted")
    }
    
    val graph: Graph[Any, Double] = Graph.fromEdges(edgeRDD, "")
    logger.info("The graph contains " + graph.numEdges + " edges and " +
      graph.numVertices + " vertices, partitioned onto " + edgeRDD.partitions.size)

    var neighborMap = scala.collection.Map[VertexId, Array[Neighbor]]()
    var rddNeighborMap = graph.edges.map(e => {
      (e.srcId, Neighbor(e.dstId, e.attr))
    })
    if (!is_directed) {
      val reverseMap = graph.edges.map(e => (e.dstId, Neighbor(e.srcId, e.attr)))
      rddNeighborMap = (rddNeighborMap ++ reverseMap)
    }
    val groupedNeighborMap = rddNeighborMap.groupByKey()
    neighborMap = groupedNeighborMap.map { case (x, iter) => (x, iter.toArray) }.collectAsMap()

    if (logger.isEnabledFor(Level.DEBUG)) {
      neighborMap.foreach(n => {
        var s = ""
        n._2.foreach(x => { s = s + x + ", " })
        logger.debug("Vertex " + n._1.toLong + " neighbors: " + s)
      })
    }

    var betweenness_map = Array[(Long, Double)]()

    val all_vertices = graph.vertices.map(v => v._1).collect()
    val setupTime = System.currentTimeMillis - startTime
    logger.debug("Setup time " + setupTime / 1000.0 + " seconds")
    val neighborMapB = sc.broadcast(neighborMap)
    if (!is_weighted)
      betweenness_map = sc.parallelize(all_vertices).flatMap(sourceNode => singleSourceShortestPathUnweighted(
        neighborMapB,
        sourceNode)).reduceByKey(_ + _).sortBy(-_._2).collect()
    else
      betweenness_map = sc.parallelize(all_vertices).flatMap(sourceNode => singleSourceShortestPathWeighted(
        neighborMapB, sourceNode)).reduceByKey(_ + _).sortBy(-_._2).collect()

    if (!is_directed) //divide by two the BC values if undirected
      for (i <- 0 until betweenness_map.length)
        betweenness_map(i) = (betweenness_map(i)._1, betweenness_map(i)._2 / 2.0)
    val betweennesstime = System.currentTimeMillis - startTime
    val directory = new File(output_folder)
    if (!directory.exists()) {
      directory.mkdirs();
    }
    writeBetweennessCentralityToFile(output_folder, "result.csv", betweenness_map)
    logger.info("*** Brandes exact betweenness centrality computed in " + (betweennesstime / 1000.0) + " s")
    val file = new PrintWriter(new FileWriter(new File(output_folder, "time_stats.csv"), false))
    file.write("elapsed_time," + (betweennesstime / 1000.0 + "\n"))
    file.write("num_nodes," + graph.numVertices + "\n")
    file.write("num_edges," + graph.numEdges + "\n")
    file.close
  }

  def singleSourceShortestPathUnweighted(
    neighborMapB: Broadcast[scala.collection.Map[Long, Array[Neighbor]]],
    sourceNode:   Long) = {
    val (predecessors_sigma_distance, partial_betweenness, stack) = initialization(sourceNode, false)
    traversalStepBFS(neighborMapB, sourceNode, predecessors_sigma_distance, stack)
    accumulationStep(sourceNode, predecessors_sigma_distance, partial_betweenness, stack)
    partial_betweenness
  }

  def singleSourceShortestPathWeighted(
    neighborMapB: Broadcast[scala.collection.Map[Long, Array[Neighbor]]],
    sourceNode:   Long) = {
    val (predecessors_sigma_distance, partial_betweenness, stack) = initialization(sourceNode, true)
    traversalStepDijkstra(neighborMapB, sourceNode, predecessors_sigma_distance, stack)
    accumulationStep(sourceNode, predecessors_sigma_distance, partial_betweenness, stack)
    partial_betweenness
  }

  def initialization(sourceNode: Long, is_weighted: Boolean) = {
    val predecessors_sigma_distance = scala.collection.mutable.Map[Long, (Array[Long], Double, Double)]()
    val partial_betweenness = scala.collection.mutable.Map[Long, Double]()
    val stack = Stack[Long]()
    //for current node set sigma to 1 and dist to 0
    val tuple = (Array[Long](), 1.0, 0.0)
    predecessors_sigma_distance(sourceNode) = tuple
    (predecessors_sigma_distance, partial_betweenness, stack)
  }

  def traversalStepBFS(
    neighborMap:                 Broadcast[scala.collection.Map[Long, Array[Neighbor]]],
    sourceNode:                  Long,
    predecessors_sigma_distance: scala.collection.mutable.Map[Long, (Array[Long], Double, Double)],
    stack:                       Stack[Long]) = {
    logger.debug("Using BFS to compute shortest paths from source node: " + sourceNode)
    val queue = Queue[Long]()
    queue.enqueue(sourceNode)
    while (!queue.isEmpty) {
      val v = queue.dequeue
      stack.push(v)
      //neighbor of extracted node evaluation
      neighborMap.value.getOrElse(v, Array()).foreach(w => {
        //first time visit
        if (predecessors_sigma_distance.getOrElseUpdate(w.id, (Array[Long](), 0l, -1))._3 < 0) {
          //insert node in queue
          queue.enqueue(w.id)
          //update distance
          val tuple_new_distance = (
            predecessors_sigma_distance(w.id)._1,
            predecessors_sigma_distance(w.id)._2, predecessors_sigma_distance(v)._3 + 1)
          predecessors_sigma_distance(w.id) = tuple_new_distance
        }
        //found a shortest path via v?
        if (predecessors_sigma_distance(w.id)._3 == predecessors_sigma_distance(v)._3 + 1) {
          //update sigma and add v to neighnode predecessor list
          val tuple_new_sigma_and_predecessor = (predecessors_sigma_distance(w.id)._1 :+ (v),
            predecessors_sigma_distance(w.id)._2 + predecessors_sigma_distance(v)._2,
            predecessors_sigma_distance(w.id)._3)
          predecessors_sigma_distance(w.id) = tuple_new_sigma_and_predecessor
        }
      })
    }
  }

  def neighborOrder(swd: ShortestWeightedDistanceTuple) = -swd.distance

  def traversalStepDijkstra(
    neighborMap:                 Broadcast[scala.collection.Map[Long, Array[Neighbor]]],
    sourceNode:                  Long,
    predecessors_sigma_distance: scala.collection.mutable.Map[Long, (Array[Long], Double, Double)],
    stack:                       Stack[Long]) = {
    logger.debug("Using Dijkstra to compute shortest paths from source node: " + sourceNode)
    val queue = PriorityQueue[ShortestWeightedDistanceTuple]()(Ordering.by(neighborOrder))
    queue.enqueue(ShortestWeightedDistanceTuple(sourceNode, sourceNode, 0))
    val distanceMap = scala.collection.mutable.Map[Long, Double]()
    while (!queue.isEmpty) {
      val v = queue.dequeue
      if (!distanceMap.contains(v.id)) {
        val tuple_new_sigma = (
          predecessors_sigma_distance(v.id)._1,
          predecessors_sigma_distance(v.id)._2 + predecessors_sigma_distance(v.predecessorId)._2,
          predecessors_sigma_distance(v.id)._3)
        predecessors_sigma_distance(v.id) = tuple_new_sigma
        stack.push(v.id)
        distanceMap(v.id) = v.distance
        //neighbor of extracted node evaluation
        neighborMap.value.getOrElse(v.id, Array()).foreach(w => {
          val vw_dist = v.distance + w.distance
          if (!distanceMap.contains(w.id) && (!predecessors_sigma_distance.contains(w.id) ||
            vw_dist < predecessors_sigma_distance(w.id)._3)) {
            //insert node in queue
            queue.enqueue(ShortestWeightedDistanceTuple(w.id, v.id, vw_dist))
            //update distance
            val tuple_new_distance = (Array(v.id), 0.0, vw_dist)
            predecessors_sigma_distance(w.id) = tuple_new_distance
          } else if (predecessors_sigma_distance(w.id)._3 == vw_dist) {
            //update sigma and add v to neighnode predecessor list
            val tuple_new_sigma_and_predecessor = (predecessors_sigma_distance(w.id)._1 :+ (v.id),
              predecessors_sigma_distance(w.id)._2 + predecessors_sigma_distance(v.id)._2,
              predecessors_sigma_distance(w.id)._3)
            predecessors_sigma_distance(w.id) = tuple_new_sigma_and_predecessor
          }
        })
      }
    }
  }

  def accumulationStep(
    sourceNode:                  Long,
    predecessors_sigma_distance: scala.collection.mutable.Map[Long, (Array[Long], Double, Double)],
    partial_betweenness:         scala.collection.mutable.Map[Long, Double],
    stack:                       scala.collection.mutable.Stack[Long]) = {
    while (!stack.isEmpty) {
      val w = stack.pop
      val coeff = 1 + partial_betweenness.getOrElseUpdate(w, 0.0)
      predecessors_sigma_distance(w)._1.foreach(v => {
        //update delta (partial betwenness contribute from this source)
        val update_delta = partial_betweenness.getOrElseUpdate(v, 0.0) +
          ((predecessors_sigma_distance(v)._2.toDouble / predecessors_sigma_distance(w)._2.toDouble) *
            coeff)
        partial_betweenness(v) = update_delta
      })
    }
    //set betweenness of source node to 0
    partial_betweenness(sourceNode) = 0.0
  }

  def writeBetweennessCentralityToFile(dir: String, filename: String, centrality: Array[(Long, Double)]) = {
    val file = new PrintWriter(new FileWriter(new File(dir, filename), false))
    centrality.foreach(cn => file.write(cn._1 + " " + "%.20f".format(cn._2) + "\n"))
    file.close()
  }
}
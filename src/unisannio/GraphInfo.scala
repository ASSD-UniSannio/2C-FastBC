package unisannio

import scala.collection.mutable.Stack
import scala.collection.mutable.Queue
import scala.collection.immutable.Map
import java.io.PrintWriter
import java.io.FileWriter
import java.io.File
import scala.collection.mutable.PriorityQueue
import org.apache.spark.broadcast.Broadcast
import org.apache.log4j.Logger

class GraphInfo(
  neighborMapB:         Broadcast[scala.collection.Map[Long, Array[Neighbor]]],
  clustersNeighborMapB: Broadcast[scala.collection.immutable.Map[Long, scala.collection.immutable.Map[Long, Array[Neighbor]]]],
  bordernodesB:         Broadcast[scala.collection.Map[Long, scala.collection.immutable.Map[Long, Boolean]]]) {

  def this(neighbors: Broadcast[scala.collection.Map[Long, Array[Neighbor]]]) = this(neighbors, null, null)
  def this(
    clustersNeighbors: Broadcast[scala.collection.immutable.Map[Long, scala.collection.immutable.Map[Long, Array[Neighbor]]]],
    bordernodes:       Broadcast[scala.collection.Map[Long, scala.collection.immutable.Map[Long, Boolean]]]) = this(null, clustersNeighbors, bordernodes)

  val logger = Logger.getLogger("GraphInfo")

  def singleSourceShortestPathUnweighted(sourceNode: Long) = {
    val (predecessors_sigma_distance, partial_betweenness, stack) = initialization(sourceNode, false)
    traversalStepBFS(neighborMapB.value, sourceNode, predecessors_sigma_distance, stack)
    accumulationStep(sourceNode, predecessors_sigma_distance, partial_betweenness, stack)
    partial_betweenness
  }

  def modifiedSingleSourceShortestPathUnweighted(
    sourceNode: Long, comm: Long = -1,
    file_prefix: String = null) = {
    val (predecessors_sigma_distance, partial_betweenness, stack) = initialization(sourceNode, false)
    traversalStepBFS(clustersNeighborMapB.value(comm), sourceNode, predecessors_sigma_distance, stack)
    accumulationStep(sourceNode, predecessors_sigma_distance, partial_betweenness, stack)
    val sigma_normalized_distance_to_borders = sigmaDistanceToBordernodes(sourceNode, bordernodesB.value(comm),
      predecessors_sigma_distance, comm, file_prefix)
    (sourceNode, partial_betweenness, sigma_normalized_distance_to_borders)
  }

  def singleSourceShortestPathWeighted(
    sourceNode: Long) = {
    val (predecessors_sigma_distance, partial_betweenness, stack) = initialization(sourceNode, true)
    traversalStepDijkstra(neighborMapB.value, sourceNode, predecessors_sigma_distance, stack)
    accumulationStep(sourceNode, predecessors_sigma_distance, partial_betweenness, stack)
    partial_betweenness
  }

  def modifiedSingleSourceShortestPathWeighted(
    sourceNode: Long, comm: Long = -1,
    file_prefix: String = null) = {
    val (predecessors_sigma_distance, partial_betweenness, stack) = initialization(sourceNode, true)
    traversalStepDijkstra(clustersNeighborMapB.value(comm), sourceNode, predecessors_sigma_distance, stack)
    accumulationStep(sourceNode, predecessors_sigma_distance, partial_betweenness, stack)
    val sigma_normalized_distance_to_borders = sigmaDistanceToBordernodes(sourceNode, bordernodesB.value(comm),
      predecessors_sigma_distance, comm, file_prefix)
    (sourceNode, partial_betweenness, sigma_normalized_distance_to_borders)
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
    neighborMap:                 scala.collection.Map[Long, Array[Neighbor]],
    sourceNode:                  Long,
    predecessors_sigma_distance: scala.collection.mutable.Map[Long, (Array[Long], Double, Double)],
    stack:                       Stack[Long]) = {
    //logger.debug("Using BFS to compute shortest paths from source node: " + sourceNode)
    val queue = Queue[Long]()
    queue.enqueue(sourceNode)
    while (!queue.isEmpty) {
      val v = queue.dequeue
      stack.push(v)
      //neighbor of extracted node evaluation
      neighborMap.getOrElse(v, Array()).foreach(w => {
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
    neighborMap:                 scala.collection.Map[Long, Array[Neighbor]],
    sourceNode:                  Long,
    predecessors_sigma_distance: scala.collection.mutable.Map[Long, (Array[Long], Double, Double)],
    stack:                       Stack[Long]) = {
    //logger.debug("Using Dijkstra to compute shortest paths from source node: " + sourceNode)
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
        neighborMap.getOrElse(v.id, Array()).foreach(w => {
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

  def printStats(file_prefix: String, bordernodes: scala.collection.immutable.Map[Long, Boolean], comm: Long, the_node: Long, sigma_normalized_distance_to_borders: scala.collection.immutable.Iterable[(Long, Double, Double)]) = {
    val file = new PrintWriter(new FileWriter(new File("./" + file_prefix, "sigma_normalized_distances_for_louvain.csv"), true))
    file.write("Invoked from source node: " + the_node + ",Comm:" + comm + ",bordernodes:" + bordernodes.keys.mkString(",") + "\n" +
      the_node + ":" + sigma_normalized_distance_to_borders.toArray.mkString(", ") + "\n")
    file.close()
  }

  def oldSigmaDistanceToBordernodes(the_node: Long, bordernodes: scala.collection.immutable.Map[Long, Boolean],
                                    predecessors_sigma_distance: scala.collection.mutable.Map[Long, (Array[Long], Double, Double)],
                                    comm:                        Long, file_prefix: String) = {
    val pred_sigma_distance_to_borders = predecessors_sigma_distance.filterKeys(node => bordernodes.contains(node))
    var sigma_normalized_distance_to_borders: Iterable[(Long, Double, Double)] = null
    if (pred_sigma_distance_to_borders.isEmpty) {
      sigma_normalized_distance_to_borders = Iterable[(Long, Double, Double)]((comm, 0.0, Double.NaN))
    } else {
      val min_distance = pred_sigma_distance_to_borders.minBy(_._2._3)._2._3
      sigma_normalized_distance_to_borders = bordernodes.map(bnode => {
        val the_tuple = pred_sigma_distance_to_borders.getOrElse(bnode._1, (null, 0.0, Double.NaN))
        (comm, the_tuple._2, the_tuple._3 - min_distance)
      })
    }
    sigma_normalized_distance_to_borders
  }

  def sigmaDistanceToBordernodes(the_node: Long, bordernodes: scala.collection.Map[Long, Boolean],
                                 predecessors_sigma_distance: scala.collection.mutable.Map[Long, (Array[Long], Double, Double)],
                                 comm:                        Long, file_prefix: String) = {
    if (bordernodes.isEmpty)
      Iterable[(Long, Double, Double)]()
    else {
      val pred_sigma_distance_to_borders = predecessors_sigma_distance.filterKeys(node => bordernodes.contains(node))
      var sigma_normalized_distance_to_borders: Iterable[(Long, Double, Double)] = null

      if (pred_sigma_distance_to_borders.isEmpty) {
        val bMap = bordernodes.map(b => (b._1, (comm, 0.0, Double.NaN)))
        bMap.values
      } else {
        var sigma_normalized_distance_to_borders: Iterable[(Long, Double, Double)] = null
        //val VERY_FAR_DISTANCE = 2 * max_cluster_distance + 1

        val min_distance = pred_sigma_distance_to_borders.minBy(_._2._3)._2._3
        sigma_normalized_distance_to_borders = bordernodes.map(bnode => {
          val the_tuple = pred_sigma_distance_to_borders.getOrElse(bnode._1, (null, 0.0, Double.NaN))
          (comm, the_tuple._2, the_tuple._3 - min_distance)
        })

        sigma_normalized_distance_to_borders
      }
    }
  }
}
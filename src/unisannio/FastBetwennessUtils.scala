package unisannio

import java.io.PrintWriter
import java.io.FileWriter
import java.io.File
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast

class FastBetwennessUtils(
  sourcesClassesB: Broadcast[scala.collection.Map[Long, scala.collection.immutable.Map[Long, Boolean]]],
  localContrB:     Broadcast[scala.collection.Map[Long, scala.collection.mutable.Map[Long, Double]]],
  bordernodesB:    Broadcast[scala.collection.Map[Long, scala.collection.immutable.Map[Long, Boolean]]]) {

  def this() = this(null, null, null)

  val logger = Logger.getLogger("FastBetwennessUtils")

  def findBorderNodes(
    graph:   scala.collection.Map[Long, Array[Neighbor]],
    cluster: (Long, scala.collection.immutable.Map[Long, Array[Neighbor]])) = {
    var bordernodes = Array[(Long, Boolean)]()
    val nodes_with_incident_edges_from_cluster = cluster._2.flatMap(x => x._2.map(x => x.id)).toSet
    cluster._2.keys.foreach(node => {
      val the_node_neighbors = graph.getOrElse(node, Array[Neighbor]())
      val the_node_cluster_neighbors = cluster._2(node)
      //need to add + 1 because a cluster contains the node itself, which is not the case for the neighbor list
      if (cluster._2(node).length != graph.getOrElse(node, Array[Neighbor]()).length) {
        bordernodes = bordernodes :+ ((node, true))
      }
      /*
      if (cluster._2(node).length > 0 &&
        cluster._2(node).length != graph.getOrElse(node, Array[Neighbor]()).length )
        //&& nodes_with_incident_edges_from_cluster.contains(node)) //{
        bordernodes = bordernodes :+ ((node, true))
      //println("Node " + node + " has some incident neighbors in cluster " + cluster._1 + " so it will be added to the border nodes")
      //}else
      //  println("Node " + node + " has no incident neighbors in cluster " + cluster._1 + " so it won't be added to the border nodes")*/
    })
    (cluster._1, bordernodes.toMap)
  }

  //calc betwenness based on class
  def parNoLocalBetwenness(
    source_node: Long,
    par_g:       scala.collection.mutable.Map[Long, Double],
    comm:        Long) = {
    //var start = System.currentTimeMillis()
    val nodes_class = sourcesClassesB.value(source_node)
    val cardinality = nodes_class.size
    val local_b = localContrB.value(source_node)
    val bordernodes = bordernodesB.value(comm)

    //remove local contribute from this source
    par_g.foreach(n => {
      if (!nodes_class.contains(n._1)) {
        val update = (n._2 - local_b.getOrElse(n._1, 0.0)) * (1 + cardinality)
        par_g(n._1) = if (update > 0) update else 0.0
      } else if (bordernodes.contains(n._1)) {
        val update = (n._2 - local_b.getOrElse(n._1, 0.0)) * (cardinality)
        par_g(n._1) = if (update > 0) update else 0.0
      } else {
        par_g(n._1) = 0.0
      }
    })
    par_g
  }

  def selectSource(cn: Array[Long], local_betweenness: scala.collection.Map[Long, Double], bordernodes: scala.collection.immutable.Map[Long, Boolean]) = {
    var source = (0l, 0.0)
    val pre_source = cn.filterNot(n => bordernodes.contains(n))
    if (pre_source.isEmpty) {
      source = cn.map(n => (n, local_betweenness(n))).minBy(_._2)
    } else {
      source = pre_source.map(n => (n, local_betweenness(n))).minBy(_._2)
    }
    (source._1, cn.diff(Array(source._1)).map(s => (s, true)).toMap)
  }

}
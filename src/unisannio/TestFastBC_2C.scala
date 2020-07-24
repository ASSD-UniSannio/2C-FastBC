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
import java.nio.file.Files
import louvain.LouvainMethod
import louvain.LouvainConfig
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast

object TestFastBC_2C {
  BasicConfigurator.configure();
  val logger = Logger.getLogger("FastBC_2C")
  logger.setLevel(Level.INFO)

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)
  Logger.getLogger("io.netty").setLevel(Level.ERROR)
  val VERSION = "dynamic_clustering"

  def main(args: Array[String]) {
    if (args.length < 7) {
      logger.error("Usage: FastBC <master> <graph_filename> <edge_delimiter> <is_directed> <is_weighted>" +
        " <n_mappers> <epsilon precision> <output_prefix> <class_fraction> <num_iterations_kmeans>")
      System.exit(1)
    }

    val mapper = args(5).toInt
    val louvain_epsilon = args(6).toDouble
    var file_prefix = ""
    val classFraction = args(8).toDouble
    val num_iterations_kmeans = args(9).toInt

    file_prefix = args(7) + "/" + VERSION + "_" + classFraction + "_class_fraction/"
    logger.info("*** Executing FastBC_2C with mappers: " + mapper + ", louvain_espilon: " + louvain_epsilon
      + ", log: " + logger.getLevel + " k_fraction: " + classFraction + ", num_iterations: " + num_iterations_kmeans)

    logger.info("Writing to " + file_prefix)
    val conf = new SparkConf()
    /*.setMaster(args(0))
      .setAppName("FastBC")
      .set("spark.network.timeout", "100000000")
      .set("spark.driver.maxResultSize", "50g")
      .set("spark.executor.heartbeatinterval", "100000000")
      .set("spark.driver.heartbeatinverval", "100000000")
      .set("spark.driver.memory", "250g")
      .set("spark.executor.memory", "250g")*/
    val sc = new SparkContext(conf)

    val directory = new File(file_prefix);
    if (!directory.exists()) {
      directory.mkdirs();
    }

    val edgeFile = args(1)
    val edgedelimiter = args(2)
    var is_directed = args(3).equals("d")
    var is_weighted = args(4).equals("w")

    //reading dataset
    var startTime = System.currentTimeMillis
    val inputHashFunc = (id: String) => id.toLong
    var edgeRDD = sc.textFile(edgeFile).map(row => {
      val tokens = row.split(edgedelimiter).map(_.trim())
      tokens.length match {
        case 2 => { new Edge(inputHashFunc(tokens(0)), inputHashFunc(tokens(1)), 1.0) }
        case 3 => { new Edge(inputHashFunc(tokens(0)), inputHashFunc(tokens(1)), tokens(2).toDouble) }
        case _ => { throw new IllegalArgumentException("invalid input line: " + row) }
      }
    })

    edgeRDD = edgeRDD.coalesce(mapper, shuffle = true)
    val graph: Graph[Any, Double] = Graph.fromEdges(edgeRDD, "")
    logger.info("The graph contains " + graph.numEdges + " edges and " +
      graph.numVertices + " vertices, partitioned onto " + edgeRDD.partitions.size + " partitions for Louvain clustering.")

    val louvainSetupStartTime = System.currentTimeMillis
    var neighborMap = scala.collection.Map[VertexId, Array[Neighbor]]()
    var rddNeighborMap = graph.edges.map(e => (e.srcId, Neighbor(e.dstId, e.attr)))
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

    val louvainSetupTime = System.currentTimeMillis - louvainSetupStartTime
    logger.info("*** Louvain setup time " + louvainSetupTime / 1000.0 + " seconds")

    val louvainStartTime = System.currentTimeMillis
    val runner = new LouvainMethod()
    val (n2c, maximumWeight) = runner.run(graph, sc, louvain_epsilon, is_weighted, new LouvainConfig(file_prefix))

    val clusters = clustersGeneration(sc, neighborMap, n2c)

    if (logger.isEnabledFor(Level.DEBUG)) {
      logger.debug("After invertion:")
      neighborMap.foreach(n => {
        var s = ""
        n._2.foreach(x => { s = s + x + ", " })
        logger.debug("Vertex " + n._1.toLong + " neighbors: " + s)
      })
    }

    val louvainTime = System.currentTimeMillis - louvainStartTime
    logger.info("*** Louvain time " + louvainTime / 1000.0 + " seconds")

    val clusters_rdd = sc.parallelize(clusters)
    val clusters_mapped = clusters.toMap

    //Border nodes identification
    //broadcast of clusters
    val clusters_mapped_b = sc.broadcast(clusters_mapped)

    //Generation of text files with cluster info
    if (logger.isEnabledFor(Level.DEBUG)) {
      val cluster_comp = clusters_rdd.map(cluster => (cluster._1, cluster._2.keys))
      val cluster_comp_collected = cluster_comp.collect
      val file = new PrintWriter(new FileWriter(new File(file_prefix, "cluster_composition.csv"), false))
      cluster_comp_collected.foreach(cd => {
        file.write(cd._1 + ":" + cd._2.mkString(",") + "\n")
      })
      file.close
    }

    val bnodes_1 = System.currentTimeMillis
    val idcluster_bordernodes = clusters_rdd.map(cluster => new FastBetwennessUtils().
      findBorderNodes(neighborMap, cluster)).collectAsMap
    var num_border_nodes = 0
    idcluster_bordernodes.foreach(id_to_border => {
      num_border_nodes += id_to_border._2.keys.size
    })
    val bnodes_2 = System.currentTimeMillis
    logger.info("*** Searching for border nodes (over " + clusters_mapped.size + " clusters) took " + (bnodes_2 - bnodes_1) / 1000.0 + " s")
    logger.info("*** Number border nodes: " + num_border_nodes)
    if (logger.isEnabledFor(Level.DEBUG)) {
      val file3 = new PrintWriter(new FileWriter(new File(file_prefix, "cluster_border_nodes.csv"), false))
      idcluster_bordernodes.foreach(id_to_border => {
        file3.write(id_to_border._1 + ":" + id_to_border._2.keys.mkString(",") + "\n")
      })
      file3.close
    }

    //Finding classes and shortest paths
    val local_bc1 = System.currentTimeMillis
    if (logger.isEnabledFor(Level.DEBUG)) {
      logger.debug("Removing any existing files used in append mode...")
      var file_to_remove = new File(file_prefix + "/sigma_normalized_distances_for_louvain.csv");
      var result = Files.deleteIfExists(file_to_remove.toPath());
      if (result) {
        logger.debug("File sigma_normalized_distances_for_louvain.csv removed!")
      } else {
        logger.debug("File sigma_normalized_distances_for_louvain.csv not existing!")
      }
      file_to_remove = new File(file_prefix + "/par_no_local_bc.csv");
      result = Files.deleteIfExists(file_to_remove.toPath());
      if (result) {
        logger.debug("File par_no_local_bc.csv removed!")
      } else {
        logger.debug("File par_no_local_bc.csv not existing!")
      }
      file_to_remove = new File(file_prefix + "/single_source_bc.csv");
      result = Files.deleteIfExists(file_to_remove.toPath());
      if (result) {
        logger.debug("File single_source_bc.csv removed!")
      } else {
        logger.debug("File single_source_bc.csv not existing!")
      }
      //idcluster_bordernodes.flatMap(b=>b._2.keys).foreach(println)
    }
    val idcluster_bordernodesB = sc.broadcast(idcluster_bordernodes)

    //evaluate local cluster stats
    val (class_node, local_betweenness_rdd, local_stats) = localClusterStatsEvaluation(sc, n2c,
      clusters_mapped_b, idcluster_bordernodesB, file_prefix, is_weighted, num_iterations_kmeans,
      classFraction,
      mapper, maximumWeight)

    val local_betweenness = local_betweenness_rdd.collectAsMap

    val sources_classes_rdd = class_node.map(cn => new FastBetwennessUtils().selectSource(
      cn.toArray, local_betweenness, idcluster_bordernodes(n2c(cn.toArray.apply(0)))))
    //sources_classes_rdd.persist()
    val sources_classes = sources_classes_rdd.collectAsMap
    val sources_classesB = sc.broadcast(sources_classes)
    //println("Identified source classes: ")
    //sources_classes.foreach(x => { println(x._1 + ": " + x._2.keys.mkString(", ")) })

    val local_bc2 = System.currentTimeMillis
    logger.info("*** Class detection took " + (local_bc2 - local_bc1) / 1000.0 + " s")

    //Generate classes
    logger.info("*** Number of classes required: " + sources_classes.size)
    val file_stats = new PrintWriter(new FileWriter(new File(file_prefix, "other_stats.csv"), false))
    file_stats.write("num_pivots," + sources_classes.size + "\n")
    if (logger.isEnabledFor(Level.DEBUG)) {
      val file4 = new PrintWriter(new FileWriter(new File(file_prefix, "source_classnodes.csv"), false))
      sources_classes.foreach(cn => {
        file4.write(cn._1 + ":" + cn._2.keys.mkString(",") + "\n")
      })
      file4.close
    }

    //execute Brandes on selected nodes
    val brandes_1 = System.currentTimeMillis
    val neighborMapB = sc.broadcast(neighborMap)
    var par_graph_betwenness: RDD[(Long, scala.collection.mutable.Map[Long, Double])] = null
    if (!is_weighted)
      par_graph_betwenness = sources_classes_rdd.map(source_node_class =>
        (
          source_node_class._1,
          new GraphInfo(neighborMapB).singleSourceShortestPathUnweighted(source_node_class._1)))
    else
      par_graph_betwenness = sources_classes_rdd.map(source_node_class =>
        (
          source_node_class._1,
          new GraphInfo(neighborMapB).
          singleSourceShortestPathWeighted(source_node_class._1)))

    val brandes_2 = System.currentTimeMillis
    logger.info("*** SSSPs performed over " + sources_classes.size + " pivot nodes (using "
      + sources_classes_rdd.partitions.size + " RDD partitions) took " + (brandes_2 - brandes_1) / 1000.0 + " s")

    val fix_1 = System.currentTimeMillis
    //val sources_classes_b = sc.broadcast(sources_classes)
    val local_contr = local_stats.map(ls => (ls._1, ls._2.filter(x => x._2 > 0))).filter(ls => sources_classes.contains(ls._1)).collectAsMap
    val local_contrB = sc.broadcast(local_contr)
    val betwenness_no_local = par_graph_betwenness.flatMap(par => (par._2)).reduceByKey(_ + _)

    val fix_2 = System.currentTimeMillis
    logger.info("*** Fixing partial BC values from pivot with cardinality and local BC took " + (fix_2 - fix_1) / 1000.0 + " s")

    //calc final betwenness adding local contribute
    //println("*** Reduce operation for final computation of total bc...")

    val bc_reduce_1 = System.currentTimeMillis
    val final_betwenness = (betwenness_no_local ++ local_betweenness_rdd).reduceByKey(_ + _).sortBy(-_._2).collect
    val bc_reduce_2 = System.currentTimeMillis
    logger.info("*** Betweenness Centrality reduce operation computed in " + (bc_reduce_2 - bc_reduce_1) / 1000.0 + " s")

    if (!is_directed) //divide by two the BC values if undirected
      for (i <- 0 until final_betwenness.length)
        final_betwenness(i) = (final_betwenness(i)._1, final_betwenness(i)._2 / 2.0)
    val betweennesstime = System.currentTimeMillis - startTime
    writeBetweennessCentralityToFile(file_prefix, "result.csv", final_betwenness)
    logger.info("*** Approx. betweenness Centrality computed in " + betweennesstime / 1000.0 + " s")
    file_stats.write("elapsed_time," + (betweennesstime / 1000.0 + "\n"))
    file_stats.close
  }

  def localClusterStatsEvaluation(
    sc:                    org.apache.spark.SparkContext,
    n2c:                   scala.collection.Map[Long, Long],
    clusters_mapped_b:     Broadcast[scala.collection.immutable.Map[Long, scala.collection.immutable.Map[Long, Array[Neighbor]]]],
    idcluster_bordernodes: Broadcast[scala.collection.Map[Long, scala.collection.immutable.Map[Long, Boolean]]],
    file_prefix:           String,
    is_weighted:           Boolean,
    num_iterations_kmeans: Int,
    classFraction:         Double,
    mappers:               Int,
    maximumWeight:         Double                                                                                                 = 1.0) = {

    var local_stats: RDD[(Long, scala.collection.mutable.Map[Long, Double], Iterable[(Long, Double, Double)])] = null
    if (!is_weighted) {
      local_stats = sc.parallelize(n2c.toSeq).map(node_comm =>
        new GraphInfo(clusters_mapped_b, idcluster_bordernodes).
          modifiedSingleSourceShortestPathUnweighted(node_comm._1, node_comm._2, file_prefix))
    } else {
      local_stats = sc.parallelize(n2c.toSeq).map(node_comm =>
        new GraphInfo(clusters_mapped_b, idcluster_bordernodes).
          modifiedSingleSourceShortestPathWeighted(node_comm._1, node_comm._2, file_prefix))
    }
    var local_sigma_and_distance: org.apache.spark.rdd.RDD[(Long, Int)] = null

    val preparing_time_1 = System.currentTimeMillis
    val c2n = n2c.groupBy(_._2).mapValues(_.map(_._1))
    val node_to_properties = local_stats.map(ls => (ls._1, ls._3.flatMap(x => (List(x._2, x._3).map { case (x) => if (x.isNaN) -maximumWeight else x })).toArray)).collectAsMap
    val comm_to_node_vectors = c2n.map(x => (x._1, (x._2.map(n => (n, node_to_properties(n))).toMap)))

    val comm_to_classes_by_strong_constraint = local_stats.map(ls => (n2c(ls._1), ls._3.toArray.deep.hashCode)).distinct().countByKey()

    if (logger.isEnabledFor(Level.DEBUG)) {
      logger.debug("*** Community structure...")
      for ((k, v) <- comm_to_node_vectors) for ((k2, v2) <- v) logger.debug("*** community: " + k +
        " (required classes: " + comm_to_classes_by_strong_constraint(k) + "), node: " + k2 + ", (sigma/n_dist) pairs to bordernodes: " + v2.mkString("; "))

      logger.debug("*** Community to num classes...")
      for ((k, v) <- comm_to_classes_by_strong_constraint) logger.debug("*** community: " + k + " (num required classes by strong constraint: " + v)
    }

    val preparing_time_2 = System.currentTimeMillis

    ("*** Preparing collections for k-means required " + (preparing_time_2 - preparing_time_1) / 1000.0 + " s")
    //val comm_to_num_different_classes_by_full_constraint = local_stats.map(ls => (ls._1, ls._3.toArray.deep.hashCode))
    val kmeans_time_1 = System.currentTimeMillis
    val kmeans_map = comm_to_node_vectors.map(x => new KMeansClustering().executeKMeans(sc, x._1, x._2, mappers,
      num_iterations_kmeans, file_prefix, fraction = classFraction, num_pivots_in_comm = comm_to_classes_by_strong_constraint(x._1).toInt))

    val kmeans_time_2 = System.currentTimeMillis

    logger.info("*** K-means computed in " + (kmeans_time_2 - kmeans_time_1) / 1000.0 + " s, using " + num_iterations_kmeans + " max iterations")

    val nodes_map = kmeans_map.flatMap(x => x._1)
    if (logger.isEnabledFor(Level.DEBUG)) {
      val file = new PrintWriter(new FileWriter(new File(file_prefix, "comm_to_num_clusters.csv"), false))
      file.write("comm,num_clusters\n")
      kmeans_map.foreach(c => {
        file.write(c._2 + "," + c._3 + "\n")
      })
      file.close
    }
    local_sigma_and_distance = sc.parallelize(nodes_map.toSeq)

    val class_nodes_time_1 = System.currentTimeMillis
    val class_node = local_sigma_and_distance.groupBy(_._2).map(class_nodes => class_nodes._2.map(c => c._1))
    val class_nodes_time_2 = System.currentTimeMillis
    logger.info("*** Class retrieval (groupby) computed in " + (class_nodes_time_2 - class_nodes_time_1) / 1000.0 + " s")
    val local_bc_time_1 = System.currentTimeMillis
    val local_betweenness_rdd = local_stats.flatMap(ls => ls._2).reduceByKey(_ + _)
    val local_bc_time_2 = System.currentTimeMillis
    logger.info("*** Class local bc reduce by key computed in " + (local_bc_time_2 - local_bc_time_1) / 1000.0 + " s")

    (class_node, local_betweenness_rdd, local_stats)

  }

  def writeBetweennessCentralityToFile(dir: String, filename: String, centrality: Array[(Long, Double)]) = {
    val file = new PrintWriter(new FileWriter(new File(dir, filename), false))
    centrality.foreach(cn => file.write(cn._1 + " " + "%.20f".format(cn._2) + "\n"))
    file.close()
  }

  def clustersGeneration(
    sc:          org.apache.spark.SparkContext,
    neighborMap: scala.collection.Map[Long, Array[Neighbor]],
    n2c:         scala.collection.Map[Long, Long]) = {
    //writeN2CToFile(".", "n2c", n2c)
    //cluster generation
    val comm_to_nodes = n2c.toArray.groupBy(_._2)
    val comm_to_nodes_rdd = sc.parallelize(comm_to_nodes.toSeq)
    //generating clusters from initial dataset
    //val dataset_b = sc.broadcast(neighborMap)
    val clusters = comm_to_nodes_rdd.map(comm => generateClusters(comm, neighborMap)).collect()
    clusters
  }

  def generateClusters(
    comm:        (Long, Array[(Long, Long)]),
    neighborMap: scala.collection.Map[Long, Array[Neighbor]]) = {
    val linksPreFiltered = preFiltering(comm._2, neighborMap)
    (comm._1, linksPreFiltered._1.map(link => (link.head.id, filterDataset(linksPreFiltered._2, link.tail))).toMap)
  }

  //dataset prefiltering to avoid nodes not in this cluster
  def preFiltering(
    comm:        Array[(Long, Long)],
    neighborMap: scala.collection.Map[Long, Array[Neighbor]]) = {
    var preFilteredDataset = Array[Array[Neighbor]]()
    var comm_arr = Array[Neighbor]()
    for (c <- comm) {
      comm_arr = comm_arr :+ new Neighbor(c._1, 0l)
      preFilteredDataset = preFilteredDataset :+ (new Neighbor(c._1, 0l) +:
        neighborMap.getOrElse(c._1, Array[Neighbor]()))
    }
    (preFilteredDataset, comm_arr)
  }

  //for each node avoid neighbor not in this cluster
  def filterDataset(comm: Array[Neighbor], link: Array[Neighbor]) = {
    link.intersect(comm)
  }
}
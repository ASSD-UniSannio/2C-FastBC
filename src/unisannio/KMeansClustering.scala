package unisannio

import org.apache.spark.SparkContext
import java.io.PrintWriter
import java.io.FileWriter
import java.io.File
import org.apache.spark.mllib.clustering.{ KMeans, KMeansModel, BisectingKMeans }
import org.apache.spark.mllib.linalg.Vectors
import org.apache.log4j._

class KMeansClustering() extends java.io.Serializable {
  val logger = Logger.getLogger("KMeansClustering")
  logger.setLevel(Level.INFO)

  def executeKMeans(sc: SparkContext, comm: Long, node_dataset_b: scala.collection.Map[Long, Array[Double]],
                    n_mappers: Int, numIterations: Int, prefix: String, k: Int = -1, fraction: Double = Double.NaN,
                    num_pivots_in_comm: Int = -1) = {
    val pointsMap = node_dataset_b.map(s => Vectors.dense(s._2.map(_.toDouble))).toList
    val parsedData = sc.parallelize(pointsMap, n_mappers).cache
    var the_k: Int = -1
    if (k == -1) {
      the_k = Math.round(num_pivots_in_comm * fraction).toInt
      if (the_k <= 0)
        the_k = 1
    } else {
      the_k = k
    }
    logger.debug("*** Executing k-means on community " + comm +
      " including " + node_dataset_b.size + " nodes and requiring " + num_pivots_in_comm + " pivots via FastBC." +
      "Reducing num classes to k = " + the_k + ", using #iter = " + numIterations + " iterations for K-Means.")

    if (logger.isEnabledFor(Level.DEBUG)) {
      var points = parsedData.collect()
      for (p <- points)
        logger.debug("Vector: " + p)
      logger.debug()
    }
    if (parsedData.isEmpty())
      (Map[Long, Int](), comm, 0)
    else {
      val cluster_model = KMeans.train(parsedData, the_k, numIterations)
      var point_to_cluster_hash_codes: scala.collection.Map[Long, Int] = null
      if (logger.isEnabledFor(Level.DEBUG)) {
        var point_to_cluster = node_dataset_b.map(s => (s._1, (cluster_model.predict(Vectors.dense((s._2.map(_.toDouble)))), comm)))
        for ((k, v) <- point_to_cluster) logger.debug("Node: " + k + ", cluster_id (k-means cluster, comm): (" + v._1 + ", " + v._2 + "), with hashcode: " + v.hashCode)
        point_to_cluster_hash_codes = point_to_cluster.map(s => (s._1, s._2.hashCode))
      } else {
        point_to_cluster_hash_codes = node_dataset_b.map(s => (s._1, (cluster_model.predict(Vectors.dense((s._2.map(_.toDouble)))), comm).hashCode))
      }
      //return a map like (node, (k_means_cluster, comm).hashcode, requested_k, actual_k)
      (point_to_cluster_hash_codes, comm, the_k)
    }
  }
}
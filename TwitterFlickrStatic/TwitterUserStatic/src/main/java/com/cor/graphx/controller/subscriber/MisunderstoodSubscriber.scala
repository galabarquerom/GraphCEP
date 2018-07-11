package com.cor.graphx.controller.subscriber

import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.VertexRDD
import com.cor.graphx.model.event.Misunderstood
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import com.cor.graphx.model.event.VertexProperty
import com.cor.graphx.model.event.FlickrUserEvent
import com.cor.graphx.model.event.PhotoEvent
import com.cor.graphx.model.event.NiceTwitterPhoto
import org.apache.spark.graphx.EdgeDirection
import org.apache.spark.graphx.Graph.graphToGraphOps
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

/**
 * Class MisunderstoodSubscriber
 */
class MisunderstoodSubscriber(sc: SparkContext) {

  //Misunderstood
  var verticesMisunderstood: RDD[((Long, Long), Misunderstood)] = sc.emptyRDD

  /**
   * Method misunderstood
   * Updates verticesMisunderstood
   * @param graph stores events
   * @return graph with Misunderstood events
   */
  def misunderstood(graph: Graph[(VertexProperty), (String, String, Long)], verticesNiceTP: RDD[((Long, Long), NiceTwitterPhoto)]): RDD[((Long, Long), Misunderstood)] = {
    //Subgraph that contains FlickrUsers, Photos  and relations follows (Between two FlickrUsers) and favorites (Between FlickrUser and Photo), publishes (between TwitterUser and Tweet)
    val flickrUserPhotoFilter = graph.subgraph(vpred = (id, attr) => (attr.isInstanceOf[PhotoEvent] || attr.isInstanceOf[FlickrUserEvent]), epred = t => t.toString().contains("follows") || t.toString().contains("favorites"))

    val graphNiceTP = VertexRDD(verticesNiceTP.map(rdd => (rdd._1._1, (rdd._1._2))).reduceByKey((a, b) => b))
    //Join information from graphNiceTP to flickrUserPhotoFilter
    val graphNiceTPOutDegree = flickrUserPhotoFilter.outerJoinVertices(graphNiceTP) { (id, oldAttr, counterDegrees) =>
      (oldAttr,
        counterDegrees match {
          case Some(counter) => counter
          case None          => 0 // No inDegree means zero inDegree
        })

    }

    //Select Photos from graphNiceTP and join with flickrUserPhotoFilter
    val graphNiceTPPhotos = graphNiceTPOutDegree.subgraph(vpred = (id, attr) => ((attr._1.isInstanceOf[PhotoEvent] && attr._2 > 0) || attr._1.isInstanceOf[FlickrUserEvent]), epred = t => t.toString().contains("follows") || t.toString().contains("favorites"))

    val graphFlickrUserPhotoFilter = flickrUserPhotoFilter.mask(graphNiceTPPhotos)

    //add neighbors for each vertex
    val graphWithNeighbords = graphFlickrUserPhotoFilter.outerJoinVertices(graphFlickrUserPhotoFilter.ops.collectNeighborIds(EdgeDirection.In)) {
      (id, oldAttr, neighbors) =>
        (oldAttr,
          neighbors match {
            case Some(counter) => counter
            case None          => Array() // No inDegree means zero inDegree
          })
    }
    //Select relation favorites
    val photoFlickrUserWithNeighbords = graphWithNeighbords.subgraph(epred = e => e.attr._1 == "favorites")
    val photoFlickrUserWithNeighbordsFilterEdges = photoFlickrUserWithNeighbords.subgraph(epred = e => ((e.srcAttr._2).intersect(e.dstAttr._2)).length == 0)
    val photoFlickrUserWithNeighbordsFilterEdgesDegrees = photoFlickrUserWithNeighbordsFilterEdges.outerJoinVertices(photoFlickrUserWithNeighbordsFilterEdges.degrees) {
      (id, oldAttr, counterDegrees) =>
        (oldAttr,
          counterDegrees match {
            case Some(counter) => counter
            case None          => 0 // No inDegree means zero inDegree
          })
    }
    val graphMisunderstoodAux = photoFlickrUserWithNeighbordsFilterEdgesDegrees.subgraph(vpred = (id, attr) => attr._2 > 0)

    verticesMisunderstood = (graphMisunderstoodAux.triplets.map(rdd => ((rdd.srcId, rdd.dstId), new Misunderstood(rdd.srcId, rdd.dstId, System.currentTimeMillis()))) ++ verticesMisunderstood).reduceByKey((a, b) =>
      if (a.currentTimestamp > b.currentTimestamp) {
        a
      } else {
        b
      })
      .filter { v => (System.currentTimeMillis() - v._2.asInstanceOf[VertexProperty].currentTimeStamp) < 3600000 }

    return verticesMisunderstood

  }
}
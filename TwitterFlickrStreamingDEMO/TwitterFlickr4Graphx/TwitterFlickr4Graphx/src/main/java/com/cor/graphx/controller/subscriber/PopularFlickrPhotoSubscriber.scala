package com.cor.graphx.controller.subscriber

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.cor.graphx.model.event.PopularFlickrPhoto
import org.apache.spark.graphx.Graph
import com.cor.graphx.model.event.VertexProperty
import com.cor.graphx.model.event.FlickrUserEvent
import com.cor.graphx.model.event.PhotoEvent
import org.apache.spark.graphx.Graph.graphToGraphOps
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

/**
 * Class PopularFlickrPhotoSubscriber
 */
class PopularFlickrPhotoSubscriber(sc: SparkContext) {

  // PopularTwitterPhoto with Photo and event date
  var verticesPopularFP: RDD[(Long, PopularFlickrPhoto)] = sc.emptyRDD

  /**
   * Method popularFlickrPhoto
   * Updates verticesPopularFP
   * @param graph stores events
   * @return graph with PopularFlickrPhoto events
   */
  def popularFlickrPhoto(graph: Graph[(VertexProperty), (String, String, Long)]): RDD[(Long, PopularFlickrPhoto)] = {
    val photoFlickuserFilter = graph.subgraph(vpred = (id, attr) => (attr.isInstanceOf[PhotoEvent] || attr.isInstanceOf[FlickrUserEvent]), epred = t => t.attr._1 == "favorites" || (t.attr._1 == "follows" && t.srcAttr.isInstanceOf[FlickrUserEvent]))

    val followsFilter = photoFlickuserFilter.subgraph(vpred = (id, attr) => attr.isInstanceOf[FlickrUserEvent], epred = t => t.attr._1 == "follows")

    val queryFollows = photoFlickuserFilter.outerJoinVertices(followsFilter.inDegrees) { (id, oldAttr, counterFollows) =>
      (oldAttr,
        counterFollows match {
          case Some(counter) => counter
          case None          => 0 // No inDegree means zero inDegree
        })
    }

    val favoritesFilter = queryFollows.subgraph(vpred = (id, attr) => (attr._1.isInstanceOf[PhotoEvent] || (attr._1.isInstanceOf[FlickrUserEvent] && attr._2 > 50)), epred = t => t.attr._1 == "favorites")

    val queryFavorites = photoFlickuserFilter.outerJoinVertices(favoritesFilter.inDegrees) { (id, oldAttr, counterFollows) =>
      (oldAttr,
        counterFollows match {
          case Some(counter) => counter
          case None          => 0 // No inDegree means zero inDegree
        })
    }

    val graphPopularFPAux = queryFavorites.subgraph(vpred = (id, attr) => (attr._1.isInstanceOf[PhotoEvent] && attr._2 > 50))

    //Result PopularTwitterPhoto
    verticesPopularFP = (graphPopularFPAux.vertices.map(rdd => (rdd._1, new PopularFlickrPhoto(rdd._1, System.currentTimeMillis()))) ++ verticesPopularFP).reduceByKey((a, b) =>
      if (a.currentTimestamp > b.currentTimestamp) {
        a
      } else {
        b
      })
      .filter { v => (System.currentTimeMillis() - v._2.asInstanceOf[VertexProperty].currentTimeStamp) < 3600000 }
    return verticesPopularFP
  }

}
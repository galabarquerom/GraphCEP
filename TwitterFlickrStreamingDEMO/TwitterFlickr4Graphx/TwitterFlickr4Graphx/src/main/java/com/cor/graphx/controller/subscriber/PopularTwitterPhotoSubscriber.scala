package com.cor.graphx.controller.subscriber

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.cor.graphx.model.event.PopularTwitterPhoto
import org.apache.spark.graphx.Graph
import com.cor.graphx.model.event.VertexProperty
import com.cor.graphx.model.event.TweetEvent
import com.cor.graphx.model.event.HashtagEvent
import com.cor.graphx.model.event.PhotoEvent
import com.cor.graphx.model.event.TwitterUserEvent
import scala.collection.mutable.ListBuffer

/**
 * Class PopularTwitterPhotoSubscriber
 */
class PopularTwitterPhotoSubscriber(sc: SparkContext) {

  // PopularTwitterPhoto with Photo, Hashtag and event date
  var verticesPopularTP: RDD[((Long, Long), PopularTwitterPhoto)] = sc.emptyRDD

  /**
   * Method popularTwitterPhoto
   * Updates verticesPopularTP
   * @param graph stores events
   * @return graph with PopularTwitterPhoto events
   */
  def popularTwitterPhoto(graph: Graph[(VertexProperty), (String, String, Long)]) : RDD[((Long, Long), PopularTwitterPhoto)] = {

    val hashtagTweetPhotoUserFilter = graph.subgraph(vpred = (id, attr) => (attr.isInstanceOf[HashtagEvent] || (attr.isInstanceOf[TweetEvent] && (System.currentTimeMillis() - attr.asInstanceOf[TweetEvent].date) < 360000000) || attr.isInstanceOf[PhotoEvent] || attr.isInstanceOf[TwitterUserEvent]), epred = t => t.toString().contains("contains") || t.toString().contains("tags") || t.toString().contains("likes")) //timestamp should be 3600000

    //Filter relation tags
    val photoHashtagFilter = hashtagTweetPhotoUserFilter.subgraph(vpred = (id, attr) => attr.isInstanceOf[PhotoEvent] || attr.isInstanceOf[HashtagEvent], epred = t => t.attr._1 == "tags")

    //Filter to get likes between TwitterUsers and Tweets
    val likesFilter = hashtagTweetPhotoUserFilter.subgraph(vpred = (id, attr) => attr.isInstanceOf[TweetEvent] || attr.isInstanceOf[TwitterUserEvent], epred = t => t.attr._1 == "likes" && (System.currentTimeMillis() - t.attr._3) < 360000000) // timestamp should be 3600000

    //Join the number of likes per tweet
    val queryLikes = hashtagTweetPhotoUserFilter.outerJoinVertices(likesFilter.inDegrees) { (id, oldAttr, counter) =>
      (oldAttr,
        counter match {
          case Some(counter) => counter
          case None          => 0 // No inDegree means zero inDegree
        })
    }

    // Filter to get what Tweet (with more than 30 likes) contains what hashtag
    val containsFilter = queryLikes.subgraph(vpred = (id, attr) => (attr._1.isInstanceOf[TweetEvent] && attr._2 > 30) || attr._1.isInstanceOf[HashtagEvent], epred = t => t.attr._1 == "contains")

    //Join the number of Tweets with more than 30 likes that use each hashtag
    val queryContains = hashtagTweetPhotoUserFilter.outerJoinVertices(containsFilter.inDegrees) { (id, oldAttr, counterLikes) =>
      (oldAttr,
        counterLikes match {
          case Some(counter) => counter
          case None          => 0 // No inDegree means zero inDegree
        })
    }

    //Get a graph with photos that contains a hashtag that has been used by a tweet with more than 30 likes
    val tagsFilter = queryContains.subgraph(vpred = (id, attr) => attr._1.isInstanceOf[PhotoEvent] || (attr._1.isInstanceOf[HashtagEvent] && attr._2 > 0), epred = t => t.attr._1 == "tags")

    val queryTags = hashtagTweetPhotoUserFilter.outerJoinVertices(tagsFilter.degrees) { (id, oldAttr, counterLikes) =>
      (oldAttr,
        counterLikes match {
          case Some(counter) => counter
          case None          => 0 // No inDegree means zero inDegree
        })
    }

    val photosFilter = queryTags.subgraph(vpred = (id, attr) => (attr._1.isInstanceOf[HashtagEvent] && attr._2 > 0) || (attr._1.isInstanceOf[PhotoEvent] && attr._2 > 0), epred = t => t.attr._1 == "tags")
    val graphPopularTPAux = photosFilter.mapEdges(e => (ListBuffer(e.attr._1(0)), System.currentTimeMillis()))

    //Result PopularTwitterPhoto
    verticesPopularTP = (graphPopularTPAux.triplets.map(rdd => ((rdd.srcId, rdd.dstId), new PopularTwitterPhoto(rdd.srcId, rdd.dstId, rdd.dstAttr._1.asInstanceOf[HashtagEvent].id, rdd.attr._2))) ++ verticesPopularTP).reduceByKey((a, b) =>
      if (a.currentTimestamp > b.currentTimestamp) {
        a
      } else {
        b
      })
      .filter { v => (System.currentTimeMillis() - v._2.asInstanceOf[VertexProperty].currentTimeStamp) < 3600000 }
    
      return verticesPopularTP
  }

}
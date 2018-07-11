package com.cor.graphx.controller.subscriber

import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import com.cor.graphx.model.event.VertexProperty
import com.cor.graphx.model.event.TweetEvent
import com.cor.graphx.model.event.HashtagEvent
import com.cor.graphx.model.event.PhotoEvent
import com.cor.graphx.model.event.HotTopic
import org.apache.spark.rdd.RDD

/**
 * Class HotTopicSubscriber
 */
class HotTopicSubscriber(sc: SparkContext) {

  // HotTopic with Hashtag properties and event date
  var verticesHotTopic: RDD[(Long, HotTopic)] = sc.emptyRDD

  /**
   * Method hotTopic
   * Updates verticesHotTopic
   * @param graph stores events
   * @return graph with HotTopic events
   */
  def hotTopic(graph: Graph[(VertexProperty), (String, String, Long)]): RDD[(Long, HotTopic)] = {

    val hashtagTweetPhotoFilter = graph.subgraph(vpred = (id, attr) => (attr.isInstanceOf[HashtagEvent] || (attr.isInstanceOf[TweetEvent] && (System.currentTimeMillis() - attr.asInstanceOf[TweetEvent].date) < 360000000) || (attr.isInstanceOf[PhotoEvent] && (System.currentTimeMillis() - attr.asInstanceOf[PhotoEvent].dateLastUpdate) < 360000000)), epred = t => t.toString().contains("contains") || t.toString().contains("tags")) //timestamp should be 3600000

    val hashtagTweetPhotoFilterInDegrees = hashtagTweetPhotoFilter.outerJoinVertices(hashtagTweetPhotoFilter.inDegrees) { (id, _, counter) =>
      counter match {
        case Some(counter) => counter
        case None          => 0 // No inDegree means zero inDegree
      }
    }
    val graphHotTopicAux = hashtagTweetPhotoFilterInDegrees.subgraph(vpred = (id, attr) => attr >= 100).mapVertices((id, attr) => (System.currentTimeMillis()))

    //Result HotTopic graph with Hashtags
    verticesHotTopic = (graphHotTopicAux.vertices.map(rdd => (rdd._1, new HotTopic(rdd._1, rdd._2))) ++ verticesHotTopic).reduceByKey((a, b) =>
      if (a.currentTimestamp > b.currentTimestamp) {
        a
      } else {
        b
      })
      .filter { v => (System.currentTimeMillis() - v._2.asInstanceOf[VertexProperty].currentTimeStamp) < 3600000 }

    return verticesHotTopic

  }

}
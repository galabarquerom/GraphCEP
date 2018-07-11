package com.cor.graphx.controller.subscriber

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import com.cor.graphx.model.event.NiceTwitterPhoto
import com.cor.graphx.model.event.TweetEvent
import com.cor.graphx.model.event.TwitterUserEvent
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.Edge
import com.cor.graphx.model.event.VertexProperty
import com.cor.graphx.model.event.HashtagEvent
import com.cor.graphx.model.event.PhotoEvent
import org.apache.spark.graphx.VertexId
import com.cor.graphx.util.Utils

/**
 * Class NiceTwitterPhotoSubscriber
 */
class NiceTwitterPhotoSubscriber(sc: SparkContext) {

  //NiceTwitterPhoto
  var verticesNiceTP: RDD[((Long, Long), NiceTwitterPhoto)] = sc.emptyRDD

  /**
   * Method niceTwitterPhoto
   * Updates verticesNiceTP
   * @param graph stores events
   * @return graph with NiceTwitterPhoto events
   */
  def niceTwitterPhoto(graph: Graph[(VertexProperty), (String, String, Long)]): RDD[((Long, Long), NiceTwitterPhoto)] = {

    //Subgraph that contains Hashtags, Tweets from the last hour, TwitterUsers  and relations follows (Between two TwitterUsers), contains (Between Tweet and Hashtag), publishes (between TwitterUser and Tweet)
    val userFilter = graph.subgraph(vpred = (id, attr) => (attr.isInstanceOf[HashtagEvent] || (attr.isInstanceOf[TweetEvent] && (System.currentTimeMillis() - attr.asInstanceOf[TweetEvent].date) < 360000000) || attr.isInstanceOf[TwitterUserEvent] || attr.isInstanceOf[PhotoEvent]), epred = t => t.toString().contains("contains") || t.toString().contains("publishes") || t.toString().contains("follows") || t.toString().contains("tags")) // timestamp should be 3600000

    // ------ INIT H-INDEX ------
    //Filter userFilter to get who TwitterUsers follows each TwitterUser
    val userUserFilter = userFilter.subgraph(vpred = (id, attr) => (attr.isInstanceOf[TwitterUserEvent]), epred = t => t.toString().contains("follows"))

    //Calculate the number of followers per user
    val followersPerUser = userUserFilter.outerJoinVertices(userUserFilter.inDegrees) { (id, oldAttr, counterLikes) =>
      (
        counterLikes match {
          case Some(counter) => counter
          case None          => 0 // No inDegree means zero inDegree
        })
    }
    //Add a list with the number of followers per user of the followers
    val followersPerFollower = followersPerUser.aggregateMessages[List[Long]](
      triplet => {
        // Send message to destination vertex containing a list of followers per follower
        triplet.sendToDst(List(triplet.srcAttr))
      },
      // Add followers
      (x, y) => (x ++ y) // Reduce Function
    )

    //Users with h Index
    val userHIndex = userFilter.outerJoinVertices(followersPerFollower.mapValues(followers => Utils.hIndex(followers))) { (id, oldAttr, counterLikes) =>
      (oldAttr,
        counterLikes match {
          case Some(counter) => counter
          case None          => 0 // No inDegree means zero inDegree
        })
    }
    // ----- FIN H-INDEX-----

    val filterRelevantUsers = userHIndex.subgraph(vpred = (id, attr) => (attr._1.isInstanceOf[HashtagEvent] || attr._1.isInstanceOf[TweetEvent] || (attr._1.isInstanceOf[TwitterUserEvent] && attr._2 > 50) || attr._1.isInstanceOf[PhotoEvent]), epred = t => t.toString().contains("contains") || t.toString().contains("publishes") || t.toString().contains("tags")) //

    //This graph considers only TwitterUsers with an h-index higger than 50
    val graphRelevantUsers = userFilter.mask(filterRelevantUsers)

    //Filter triplets with Photo-Hashtag relation
    val photoHashtagRelation = userFilter.subgraph(vpred = (id, attr) => attr.isInstanceOf[HashtagEvent] || attr.isInstanceOf[PhotoEvent], epred = t => t.toString().contains("tags"))

    val userTweetFilter = userFilter.aggregateMessages[VertexId](
      triplet => { // Map Function
        if (triplet.srcAttr.isInstanceOf[TwitterUserEvent] && triplet.dstAttr.isInstanceOf[TweetEvent]) {
          // Send message to destination vertex containing idTwitterUser
          triplet.sendToDst(triplet.srcId)

        }
      },
      // Add user Twitter id
      (a, b) => (b) // Reduce Function
    )

    // Join with first filter userFilter
    val addUsertoTweet = userFilter.outerJoinVertices(userTweetFilter) { (vid, oldAttr, newOpt) =>
      (oldAttr,
        newOpt match {
          case Some(outDeg) => outDeg
          case None         => None // No outDegree means zero outDegree
        })
    }
    // Get who TwitterUser has used what Hashtag
    val groupHashtagbyUser = addUsertoTweet.triplets.filter { t => t.dstAttr._1.isInstanceOf[HashtagEvent] && t.srcAttr._1.isInstanceOf[TweetEvent] }.groupBy(f => (f.srcAttr._2.asInstanceOf[VertexId], f.dstId.asInstanceOf[VertexId]))

    // Get who TwitterUser has used what Hashtag and how many times
    val numberHashtagByUser: RDD[((VertexId, VertexId), (String, String, Long))] = groupHashtagbyUser.map {
      t => (t._1, ("hashtagUser", t._2.size.toString, 0))
    }
    // Get a graph with relations between TwitterUser and hashtag, and each TwitterUser has an h-index
    val edgesHashtagByUser: RDD[Edge[(String, String, Long)]] = numberHashtagByUser.map { t => Edge(t._1._1, t._1._2, t._2) }
    val graphHashtagUsersT = userHIndex.subgraph(vpred = (id, attr) => (attr._1.isInstanceOf[HashtagEvent] || attr._1.isInstanceOf[TwitterUserEvent]), epred = e => e.dstAttr._1.isInstanceOf[HashtagEvent] && e.srcAttr._1.isInstanceOf[TwitterUserEvent])
    val graphHashtagUsersTwitter = Graph(graphHashtagUsersT.vertices, graphHashtagUsersT.edges.union(edgesHashtagByUser))

    //Filter graph to get TwitterUsers with an h-index higger than 50 that are relationated with a hashtag at least three times
    val graphRelevantUserHashtag = graphHashtagUsersTwitter.subgraph(epred = e => e.dstAttr._1.isInstanceOf[HashtagEvent] && e.srcAttr._1.isInstanceOf[TwitterUserEvent] && e.srcAttr._2 > 50 && e.attr._2.toLong > 2)

    val RelevantPhotoHashtag = photoHashtagRelation.outerJoinVertices(graphRelevantUserHashtag.inDegrees) { (id, oldAttr, counterDegrees) =>
      (oldAttr,
        counterDegrees match {
          case Some(counter) => counter
          case None          => 0 // No inDegree means zero inDegree
        })

    }
    val graphQuery4 = RelevantPhotoHashtag.subgraph(vpred = (id, attr) => (attr._1.isInstanceOf[HashtagEvent] && attr._2 != 0) || attr._1.isInstanceOf[PhotoEvent], epred = e => e.attr._1 == "tags" && e.srcAttr._1.isInstanceOf[PhotoEvent] && e.dstAttr._1.isInstanceOf[HashtagEvent] && e.dstAttr._2 != 0)

    val graphQuery4Degrees = graphQuery4.outerJoinVertices(graphQuery4.degrees) { (id, oldAttr, counterDegrees) =>
      (oldAttr,
        counterDegrees match {
          case Some(counter) => counter
          case None          => 0 // No inDegree means zero inDegree
        })

    }

    val maskQuery4 = graphQuery4Degrees.subgraph(vpred = (id, attr) => attr._2 != 0, epred = e => e.attr._1 == "tags")
    val graphNiceTPAux = maskQuery4.mapEdges(e => (e.attr._1, System.currentTimeMillis()))

    verticesNiceTP = (graphNiceTPAux.triplets.map(rdd => ((rdd.srcId, rdd.dstId), new NiceTwitterPhoto(rdd.srcId, rdd.dstId, rdd.dstAttr._1._1.asInstanceOf[HashtagEvent].id, rdd.attr._2))) ++ verticesNiceTP).reduceByKey((a, b) =>
      if (a.currentTimestamp > b.currentTimestamp) {
        a
      } else {
        b
      })
      .filter { v => (System.currentTimeMillis() - v._2.asInstanceOf[VertexProperty].currentTimeStamp) < 3600000 }

    return verticesNiceTP
  }

}
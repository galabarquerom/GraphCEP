package com.cor.graphx.controller.subscriber

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import com.cor.graphx.model.event.InfluencerTweeted
import org.apache.spark.graphx.Graph
import com.cor.graphx.model.event.VertexProperty
import com.cor.graphx.model.event.TweetEvent
import com.cor.graphx.model.event.TwitterUserEvent
import com.cor.graphx.util.Utils
/**
 * Class InfluencerTweetedSubscriber
 */
class InfluencerTweetedSubscriber(sc: SparkContext) {

  //InfluencerTweeted
  var verticesInfluencer: RDD[(Long, InfluencerTweeted)] = sc.emptyRDD

  /**
   * Method influencerTweeted
   * Updates verticesInfluencer
   * @param graph stores events
   * @return graph with InfluencerTweeted events
   */
  def influencerTweeted(graph: Graph[(VertexProperty), (String, String, Long)]): RDD[(Long, InfluencerTweeted)] = {
    //Array that contains the last 10000 Tweets
    val recentTweets = graph.subgraph(vpred = (id, attr) => (attr.isInstanceOf[TweetEvent])).vertices.sortBy(_._2.asInstanceOf[TweetEvent].date, ascending = false).map { case (id, attr) => id }.take(10000)

    // ------ INIT H-INDEX ------
    //Filter userFilter to get who TwitterUsers follows each TwitterUser
    val userUserFilter = graph.subgraph(vpred = (id, attr) => (attr.isInstanceOf[TwitterUserEvent]), epred = t => t.toString().contains("follows"))

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
    val userHIndex = graph.outerJoinVertices(followersPerFollower.mapValues(followers => Utils.hIndex(followers))) { (id, oldAttr, counterLikes) =>
      (oldAttr,
        counterLikes match {
          case Some(counter) => counter
          case None          => 0 // No inDegree means zero inDegree
        })
    }
    // ----- FIN H-INDEX-----
    val userHIndexFollowers = userHIndex.outerJoinVertices(userUserFilter.inDegrees) {
      (id, oldAttr, newAttr) =>
        (oldAttr,
          newAttr match {
            case Some(counter) => counter
            case None          => 0 // No inDegree means zero inDegree
          })
    }
    val recentTweetsUsers = userHIndexFollowers.subgraph(vpred = (id, attr) => (attr._1._1.isInstanceOf[TweetEvent] && recentTweets.contains(id)) || (attr._1._1.isInstanceOf[TwitterUserEvent] && attr._1._2 > 70 && attr._2 > 50000), epred = e => e.attr._1 == "publishes")
    val recentTweetsUsersDegrees = recentTweetsUsers.outerJoinVertices(recentTweetsUsers.degrees) {
      (id, oldAttr, newAttr) =>
        (oldAttr._1._1,
          newAttr match {
            case Some(counter) => counter
            case None          => 0 // No inDegree means zero inDegree
          })
    }
    val graphInfluencerTweetedAux = recentTweetsUsersDegrees.subgraph(vpred = (id, attr) => attr._2 != 0)

    verticesInfluencer = (graphInfluencerTweetedAux.vertices.map(rdd => (rdd._1, new InfluencerTweeted(rdd._1, System.currentTimeMillis()))) ++ verticesInfluencer).reduceByKey((a, b) =>
      if (a.currentTimestamp > b.currentTimestamp) {
        a
      } else {
        b
      })
      .filter { v => (System.currentTimeMillis() - v._2.asInstanceOf[VertexProperty].currentTimeStamp) < 3600000 }
    return verticesInfluencer
  }

}
package com.cor.graphx.controller

import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.cor.graphx.handler.FlickrTwitterEventHandler
import scala.collection.mutable.ListBuffer
import com.cor.graphx.model.event.TweetEvent
import com.cor.graphx.model.event.HashtagEvent
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.VertexId
import java.util.Properties
import java.io.FileInputStream
import com.cor.graphx.util.Constants
import com.cor.graphx.model.event.TwitterUserEvent
import com.cor.graphx.model.event.PhotoEvent
import com.cor.graphx.model.event.FlickrUserEvent

/**
 * Object TwitterFlickrController
 * Creates an initial graph and starts a DEMO of how a CEP architecture works with TwitterFlickr example
 */
object TwitterFlickrController {

  /**
   * Method start
   * Starts the application
   */
  def start {
    //Logger
    org.apache.log4j.Logger.getLogger("org").setLevel(Level.OFF)
    val logger: org.slf4j.Logger = org.slf4j.LoggerFactory.getLogger(this.getClass)

    //Spark configuration
    val conf = new SparkConf().setAppName("Graph").setMaster("local").set("spark.driver.memory", "2g")
    val sc = new SparkContext(conf)

    logger.info("Please, wait some minutes while graph is created.")

    val eventHandler = new FlickrTwitterEventHandler(sc)
    var count = eventHandler.initService()
    
    startDemo(eventHandler, count)
    
  }
  
  /**
   * Method startDemo
   * @param eventHandler
   * @param lastId last id used in the graph
   * Starts DEMO of architecture CEP
   */
  def startDemo(eventHandler: FlickrTwitterEventHandler, lastId: Long){

    var count = lastId
    val rand = scala.util.Random
    
    while (true) {

      // --------- Twitter information ----------- //
      val t = new TweetEvent(count, System.currentTimeMillis(), "tweet-" + count + "-" + count, "tweet text", System.currentTimeMillis(), "web")
      eventHandler.handleVertex(t)
      count += 1

      //Hashtag per tweet
      val numHashtagsThisTweet = rand.nextInt(Constants.avgNumHashtagsPerTweet * 2)
      for (i <- 1 to numHashtagsThisTweet) {
        val ht = new HashtagEvent(count, System.currentTimeMillis(), "hashtag" + count)
        eventHandler.handleVertex(ht)
        count += 1
        val edge = Edge(t.idNode.asInstanceOf[VertexId], ht.idNode.asInstanceOf[VertexId], ("contains","", Constants.defaultLong))
        eventHandler.handleEdge(edge)
      }
      //Favorites per tweet
      val numLikesThisTweet = rand.nextInt(Constants.avgNumLikesPerTweet * 2)
      for (it <- 1 to numLikesThisTweet) {
        val tu2 = new TwitterUserEvent(count, System.currentTimeMillis(), "twitterUser" + count, "twitterUser" + count, "malaga", "false", "", "", "user" + count + "description", System.currentTimeMillis())
        eventHandler.handleVertex(tu2)
        count = count + 1
        val like = Edge(tu2.idNode.asInstanceOf[VertexId], t.idNode.asInstanceOf[VertexId], ("likes","", System.currentTimeMillis()))
        eventHandler.handleEdge(like)
      }
      //TwitterUser
      val user = new TwitterUserEvent(count, System.currentTimeMillis(), "twitterUser" + count, "twitterUser" + count, "malaga", "false", "", "", "user" + count + "description", System.currentTimeMillis())
      eventHandler.handleVertex(user)
      count += 1
      val edge = Edge(user.idNode.asInstanceOf[VertexId], t.idNode.asInstanceOf[VertexId], ("publishes","", Constants.defaultLong))
      eventHandler.handleEdge(edge)

      //Followers per TwitterUser
      val numFollowers = rand.nextInt(Constants.avgNumFollowersPerUser * 2)
      for (it <- 1 to numFollowers) {
        val tu1 = new TwitterUserEvent(count, System.currentTimeMillis(), "twitterUser" + count, "twitterUser" + count, "malaga", "false", "", "", "user" + count + "description", System.currentTimeMillis())
        eventHandler.handleVertex(tu1)
        count = count + 1
        val edge = Edge(tu1.idNode.asInstanceOf[VertexId], user.idNode.asInstanceOf[VertexId], ("follows","", Constants.defaultLong)) //tu1 follows tu
        eventHandler.handleEdge(edge)
        val numTweetsPerFollower = rand.nextInt(Constants.avgNumTweetPerUser * 2)
        for (i <- 1 to numTweetsPerFollower) {
          //Tweet follower
          val t1 = new TweetEvent(count, System.currentTimeMillis(), "tweet-" + count + "-" + count, "tweet text", System.currentTimeMillis(), "web")
          eventHandler.handleVertex(t)
          count += 1
          val edge1 = Edge(tu1.idNode.asInstanceOf[VertexId], t1.idNode.asInstanceOf[VertexId], ("publishes","", Constants.defaultLong))
          eventHandler.handleEdge(edge1)

          //Hashtag per tweet follower
          val numHashtagsThisTweet1 = rand.nextInt(Constants.avgNumHashtagsPerTweet * 2)
          for (i <- 1 to numHashtagsThisTweet1) {
            val ht1 = new HashtagEvent(count, System.currentTimeMillis(), "hashtag" + count)
            eventHandler.handleVertex(ht1)
            count += 1
            val edge = Edge(t1.idNode.asInstanceOf[VertexId], ht1.idNode.asInstanceOf[VertexId], ("contains","", Constants.defaultLong))
            eventHandler.handleEdge(edge)
          }
        }
      }

      //Tweets per User
      val numTweetsPerUserTweet = rand.nextInt(Constants.avgNumTweetPerUser * 2)
      for (i <- 1 to numTweetsPerUserTweet) {
        //Tweet follower
        val tUser = new TweetEvent(count, System.currentTimeMillis(), "tweet-" + count + "-" + count, "tweet text", System.currentTimeMillis(), "web")
        eventHandler.handleVertex(tUser)
        count += 1
        val edge1 = Edge(user.idNode.asInstanceOf[VertexId], tUser.idNode.asInstanceOf[VertexId], ("publishes","", Constants.defaultLong))
        eventHandler.handleEdge(edge1)

        //Hashtag per tweet follower
        val numHashtagsThisTweet1 = rand.nextInt(Constants.avgNumHashtagsPerTweet * 2)
        for (i <- 1 to numHashtagsThisTweet1) {
          val ht1 = new HashtagEvent(count, System.currentTimeMillis(), "hashtag" + count)
          eventHandler.handleVertex(ht1)
          count += 1
          val edge = Edge(tUser.idNode.asInstanceOf[VertexId], ht1.idNode.asInstanceOf[VertexId], ("contains","", Constants.defaultLong))
          eventHandler.handleEdge(edge)
        }
        //Favorites per tweet
        val numLikesThisTweet = rand.nextInt(Constants.avgNumLikesPerTweet * 2)
        for (it <- 1 to numLikesThisTweet) {
          val tu = new TwitterUserEvent(count, System.currentTimeMillis(), "twitterUser" + count, "twitterUser" + count, "malaga", "false", "", "", "user" + count + "description", System.currentTimeMillis())
          eventHandler.handleVertex(tu)
          count = count + 1
          val like = Edge(tu.idNode.asInstanceOf[VertexId], tUser.idNode.asInstanceOf[VertexId], ("likes","", System.currentTimeMillis()))
          eventHandler.handleEdge(like)
        }
      }

      // --------- Flickr information ----------- //

      //Photo

      val p = new PhotoEvent(count, System.currentTimeMillis(), "photo title", "photo-" + count + "-" + count, "photo description", System.currentTimeMillis(), System.currentTimeMillis(), "cannon");
      eventHandler.handleVertex(p)
      count = count + 1

      val numHashtagsThisPhoto = rand.nextInt(Constants.avgNumHashtagsPerTweet * 2)
      for (it <- 1 to numHashtagsThisPhoto) {
        val ht = new HashtagEvent(count, System.currentTimeMillis(), "hashtag" + count)
        eventHandler.handleVertex(ht)
        count = count + 1
        val edge = Edge(p.idNode.asInstanceOf[VertexId], ht.idNode.asInstanceOf[VertexId], ("tags","", Constants.defaultLong))
        eventHandler.handleEdge(edge)
      }

      //Favorites per photo
      val numLikesThisPhoto = rand.nextInt(Constants.avgNumFavoritesPerPhoto * 2)
      for (it <- 1 to numLikesThisPhoto) {
        val fu1 = new FlickrUserEvent(count, System.currentTimeMillis(), "flickr user " + count, "flickr user " + count, "malaga")
        eventHandler.handleVertex(fu1)
        count = count + 1
        val fav = Edge(fu1.idNode.asInstanceOf[VertexId], p.idNode.asInstanceOf[VertexId], ("favorites","", System.currentTimeMillis()))
        eventHandler.handleEdge(fav)
      }

      val fu = new FlickrUserEvent(count, System.currentTimeMillis(), "flickr user " + count, "flickr user " + count, "malaga")
      eventHandler.handleVertex(fu)
      count += 1
      val edgeUser = Edge(fu.idNode.asInstanceOf[VertexId], p.idNode.asInstanceOf[VertexId], ("owns","", Constants.defaultLong))
      eventHandler.handleEdge(edgeUser)

      //Photos per user
      val numPhotos = rand.nextInt(Constants.avgNumPhotoPerUser * 2);
      for (it <- 1 to numPhotos) {
        val photo = new PhotoEvent(count, System.currentTimeMillis(), "photo title", "photo-" + fu.id + "-" + count, "photo description", System.currentTimeMillis(), System.currentTimeMillis(), "cannon");
        eventHandler.handleVertex(photo)
        count = count + 1
        val numHashtagsThisPhoto = rand.nextInt(Constants.avgNumHashtagsPerTweet * 2)
        for (it <- 1 to numHashtagsThisPhoto) {
          val ht = new HashtagEvent(count, System.currentTimeMillis(), "hashtag" + count)
          eventHandler.handleVertex(ht)
          count = count + 1
          val edge = Edge(photo.idNode.asInstanceOf[VertexId], ht.idNode.asInstanceOf[VertexId], ("tags","", Constants.defaultLong))
          eventHandler.handleEdge(edge)
        }
        val edge = Edge(fu.idNode.asInstanceOf[VertexId], photo.idNode.asInstanceOf[VertexId], ("owns","", Constants.defaultLong))
        eventHandler.handleEdge(edge)

        //Favorites per photo
        val numLikesThisPhoto = rand.nextInt(Constants.avgNumFavoritesPerPhoto * 2)
        for (it <- 1 to numLikesThisPhoto) {
          val fu1 = new FlickrUserEvent(count, System.currentTimeMillis(), "flickr user " + count, "flickr user " + count, "malaga")
          eventHandler.handleVertex(fu1)
          count = count + 1
          val fav = Edge(fu1.idNode.asInstanceOf[VertexId], photo.idNode.asInstanceOf[VertexId], ("favorites","", System.currentTimeMillis()))
          eventHandler.handleEdge(fav)
        }
      }

      val numFollowersFlickr = rand.nextInt(Constants.avgNumFollowersPerFlickrUser * 2)
      val it = 0
      for (it <- 1 to numFollowersFlickr) {
        val fu1 = new FlickrUserEvent(count, System.currentTimeMillis(), "flickr user " + count, "flickr user " + count, "malaga")
        eventHandler.handleVertex(fu1)
        count += 1
        val edge = Edge(fu1.idNode.asInstanceOf[VertexId], fu.idNode.asInstanceOf[VertexId], ("follows","", Constants.defaultLong)) //tu1 follows tu
        eventHandler.handleEdge(edge)
      }

    }

  }
}
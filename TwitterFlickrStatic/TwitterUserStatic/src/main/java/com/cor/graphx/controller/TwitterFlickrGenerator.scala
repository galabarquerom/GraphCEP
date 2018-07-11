package com.cor.graphx.controller

import com.cor.graphx.model.event.FlickrUserEvent
import com.cor.graphx.model.event.HashtagEvent
import com.cor.graphx.model.event.TwitterUserEvent
import com.cor.graphx.model.event.VertexProperty
import org.apache.spark.SparkContext
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.Edge
import com.cor.graphx.model.event.TweetEvent
import com.cor.graphx.model.event.PhotoEvent
import org.apache.spark.graphx.VertexId
import java.util.Properties
import java.io.FileInputStream
import com.cor.graphx.util.Constants

/**
 * Class TwitterFlickrGenerator
 */
class TwitterFlickrGenerator(sc: SparkContext) {

  val logger: org.slf4j.Logger = org.slf4j.LoggerFactory.getLogger(this.getClass)

  // Nodes
  var nodesArray = ArrayBuffer[(VertexId, VertexProperty)]()
  var inactiveTwitterUsers = new ListBuffer[TwitterUserEvent]()
  var influencerTwitterUsers = new ListBuffer[TwitterUserEvent]()
  var activeTwitterUsers = new ListBuffer[TwitterUserEvent]()
  var flickrUsers = new ListBuffer[FlickrUserEvent]()
  var photos = new ListBuffer[PhotoEvent]()
  var tweets = new ListBuffer[TweetEvent]()
  var hashtags = new ListBuffer[HashtagEvent]()

  //Edges

  var flickrUserUserRelation = ListBuffer[Edge[(String, String, Long)]]()
  var twitterUserUserRelation = ListBuffer[Edge[(String, String, Long)]]()
  var twitterUserTweetRelation = ListBuffer[Edge[(String, String, Long)]]()
  var flickrUserPhotoRelation = ListBuffer[Edge[(String, String, Long)]]()
  var tweetHashtagEdge = ListBuffer[Edge[(String, String, Long)]]()
  var photoHashtagEdge = ListBuffer[Edge[(String, String, Long)]]()

  // Likes, Favorites...
  var likes = ListBuffer[Edge[(String, String, Long)]]()
  var mentions = ListBuffer[Edge[(String, String, Long)]]()
  var favorites = ListBuffer[Edge[(String, String, Long)]]()
  var comments = ListBuffer[Edge[(String, String, Long)]]()

  /**
   * Method generateGraph
   * @return graph created
   * Creates the graph with nodes and edges
   */
  def generateGraph(): Graph[VertexProperty, (String, String, Long)] = {

    //Selecting properties
    val (numInactiveTwitterUsers, numInfluencerTwitterUsers, numActiveTwitterUsers, numFlickrUsers, numHashtags) = try {
      val prop = new Properties()
      prop.load(new FileInputStream("config.properties"))

      (
        prop.getProperty("numInactiveTwitterUsers").toInt,
        prop.getProperty("numInfluencerTwitterUsers").toInt,
        prop.getProperty("numActiveTwitterUsers").toInt,
        prop.getProperty("numFlickrUsers").toInt,
        prop.getProperty("numHashtags").toInt)
    } catch {
      case e: Exception =>
        logger.info("Configuration file not found")
        sys.exit(1)
    }

    var nextVertexNum = 0L

    //Creating TwitterUsers
    nextVertexNum = createTwitterUsers(nextVertexNum, numInactiveTwitterUsers, numActiveTwitterUsers, numInfluencerTwitterUsers)

    //Creating FlickrUsers
    nextVertexNum = createFlickrUsers(nextVertexNum, numFlickrUsers)

    //Creating hashtags
    nextVertexNum = createHashtags(nextVertexNum, numHashtags)

    //Creating follows from twitter
    val allUsers = createFollowsTwitter()

    //Creating follows from flickr
    createFollowsFlickr()

    //Creating Tweets
    nextVertexNum = createTweets(nextVertexNum)

    //Creating Photos
    nextVertexNum = createPhotos(nextVertexNum)

    //Creating Relation contains between tweet and hashtag
    createContains()

    //Creating Relation tags between photo and hashtag
    createTags()

    //Creating likes
    createLikes(allUsers)

    //Creating mentions
    createMentions(allUsers)

    //Creating favorites
    createFavorites()

    //Creating comments
    createComments()

    val nodes = inactiveTwitterUsers ++ activeTwitterUsers ++ influencerTwitterUsers ++ flickrUsers ++ tweets ++ hashtags ++ photos
    val edges = twitterUserUserRelation ++ flickrUserUserRelation ++ twitterUserTweetRelation ++ flickrUserPhotoRelation ++ tweetHashtagEdge ++ photoHashtagEdge ++ likes ++ mentions ++ comments ++ favorites

    val dataNodes = sc.parallelize(nodesArray)
    val dataEdges = sc.parallelize(edges)
    var graph: Graph[VertexProperty, (String, String, Long)] = Graph(dataNodes, dataEdges)
    logger.info(" Graph finished")

    clearAll()

    return graph

  }

  /**
   * Method createTwitterUsers
   * @param lastId last id node used
   * @param numInactiveTwitterUsers
   * @param numActiveTwitterUsers
   * @param numInfluencerTwitterUsers
   * Creates TwitterUsers
   */
  def createTwitterUsers(lastId: Long, numInactiveTwitterUsers: Int, numActiveTwitterUsers: Int, numInfluencerTwitterUsers: Int): Long = {

    var nextVertexNum = lastId
    //Creating InnactiveUsers from Twitter
    for (i <- 1 to numInactiveTwitterUsers) {
      val user = new TwitterUserEvent(nextVertexNum, System.currentTimeMillis(), "twitterUser" + i, "twitterUser" + i, "malaga", "false", "", "", "user" + i + "description", System.currentTimeMillis())
      inactiveTwitterUsers += user
      nodesArray = nodesArray :+ (nextVertexNum, user)
      nextVertexNum += 1
    }

    logger.info("Inactive Twitter Users created")

    //Creating ActiveUsers from Twitter
    for (x <- numInactiveTwitterUsers + 1 to (numInactiveTwitterUsers + numActiveTwitterUsers)) {
      val user = new TwitterUserEvent(nextVertexNum, System.currentTimeMillis(), "twitterUser" + x, "twitterUser" + x, "malaga", "false", "", "", "user" + x + "description", System.currentTimeMillis())
      activeTwitterUsers += user
      nodesArray = nodesArray :+ (nextVertexNum, user)
      nextVertexNum += 1
    }

    logger.info("Active Twitter Users created")

    //Creating Influencers from Twitter
    for (x <- (numInactiveTwitterUsers + numActiveTwitterUsers + 1) to (numInactiveTwitterUsers + numActiveTwitterUsers + numInfluencerTwitterUsers)) {
      val user = new TwitterUserEvent(nextVertexNum, System.currentTimeMillis(), "twitterUser" + x, "twitterUser" + x, "malaga", "false", "", "", "user" + x + "description", System.currentTimeMillis())
      influencerTwitterUsers += user
      nodesArray = nodesArray :+ (nextVertexNum, user)
      nextVertexNum += 1
    }

    logger.info("Influencer Twitter Users created")

    return nextVertexNum

  }

  /**
   * Method createFlickrUsers
   * @param lastId last id node used
   * @param numFlickrUsers
   * Creates FlickrUsers
   */
  def createFlickrUsers(lastId: Long, numFlickrUsers: Int): Long = {

    var nextVertexNum = lastId
    //Creating users from Flickr
    for (u <- 1 to numFlickrUsers) {
      val fu = new FlickrUserEvent(nextVertexNum, System.currentTimeMillis(), "flickr user " + u, "flickr user " + u, "malaga");
      flickrUsers += fu
      nodesArray = nodesArray :+ (nextVertexNum, fu)
      nextVertexNum += 1
    }

    logger.info("Flickr Users created")

    return nextVertexNum
  }

  /**
   * Method createHashtags
   * @param lastId last id node used
   * @param numHashtags
   * Creates Hashtags
   */
  def createHashtags(lastId: Long, numHashtags: Int): Long = {

    var nextVertexNum = lastId
    //Creating Hashtags
    for (p <- 1 to numHashtags) {
      val ht = new HashtagEvent(nextVertexNum, System.currentTimeMillis(), "hashtag" + p)
      hashtags += ht
      nodesArray = nodesArray :+ (nextVertexNum, ht)
      nextVertexNum += 1
    }
    logger.info("Hashtags created")

    return nextVertexNum
  }

  /**
   * Method createFollowsTwitter
   * Creates relation follows from twitter
   */
  def createFollowsTwitter() : ListBuffer[TwitterUserEvent] = {
    //Creating relation follows from Twitter
    val allUsers = inactiveTwitterUsers ++ activeTwitterUsers
    for (tu <- allUsers) {
      val rand = scala.util.Random
      val numFollowers = rand.nextInt(Constants.avgNumFollowersPerUser * 2)
      var it = 0
      for (it <- 1 to numFollowers) {
        val n = rand.nextInt(allUsers.size) //get random user
        val tu1 = allUsers(n)
        val edge = Edge(tu1.idNode.asInstanceOf[VertexId], tu.idNode.asInstanceOf[VertexId], ("follows", "", Constants.defaultLong)) //tu1 follows tu
        twitterUserUserRelation += edge
      }

    }

    //Creating relation follows from Twitter Influencers
    val allUsersWithInfluencer = allUsers ++ influencerTwitterUsers
    for (tu <- influencerTwitterUsers) {
      val rand = scala.util.Random
      val numFollowers = rand.nextInt(Constants.avgNumFollowersPerInfluencer * 2)
      var it = 0
      for (it <- 1 to numFollowers) {
        val n = rand.nextInt(allUsersWithInfluencer.size) //get random user
        val tu1 = allUsersWithInfluencer(n)
        val edge = Edge(tu1.idNode.asInstanceOf[VertexId], tu.idNode.asInstanceOf[VertexId], ("follows", "", Constants.defaultLong)) //tu1 follows tu
        twitterUserUserRelation += edge
      }
    }

    logger.info("Relation follows from Twitter is created")
    
    return allUsers

  }
  
  /**
   * Method createFollowsFlickr
   * Creates relation follows from flickr
   */
  def createFollowsFlickr(){
    //Creating relation follows from Flickr
    for (fu <- flickrUsers) {
      val rand = scala.util.Random
      val numFollowers = rand.nextInt(Constants.avgNumFollowersPerFlickrUser * 2)
      val it = 0
      for (it <- 1 to numFollowers) {
        val n = rand.nextInt(flickrUsers.size) //get random user
        val fu1 = flickrUsers(n)
        val edge = Edge(fu1.idNode.asInstanceOf[VertexId], fu.idNode.asInstanceOf[VertexId], ("follows", "", Constants.defaultLong)) //tu1 follows tu
        flickrUserUserRelation += edge
      }
    }
    logger.info("Relation follows from Flickr is created")
  }
  
  /**
   * Method createTweets
   * @param lastId last id node used
   * Creates Tweets
   */
  def createTweets(lastId: Long) : Long = {
    var nextVertexNum = lastId
    //Creating Tweets for Twitter Users
    for (tu <- activeTwitterUsers) {
      val rand = scala.util.Random
      val numTweets = rand.nextInt(Constants.avgNumTweetPerUser * 2)
      val it = 0
      for (it <- 1 to numTweets) {
        val t = new TweetEvent(nextVertexNum, System.currentTimeMillis(), "tweet-" + tu.id + "-" + it, "tweet text", System.currentTimeMillis(), "web")
        tweets += t
        val edge = Edge(tu.idNode.asInstanceOf[VertexId], t.idNode.asInstanceOf[VertexId], ("publishes", "", Constants.defaultLong))
        twitterUserTweetRelation += edge
        nodesArray = nodesArray :+ (nextVertexNum, t)
        nextVertexNum += 1
      }
    }

    //Creating Tweets for Twitter Influencers
    for (tu <- influencerTwitterUsers) {
      val rand = scala.util.Random
      val numTweets = rand.nextInt(Constants.avgNumTweetPerInfluencer * 2)
      val it = 0
      for (it <- 1 to numTweets) {
        val t = new TweetEvent(nextVertexNum, System.currentTimeMillis(), "tweet-" + tu.id + "-" + it, "tweet text", System.currentTimeMillis(), "web")
        tweets += t
        val edge = Edge(tu.idNode.asInstanceOf[VertexId], t.idNode.asInstanceOf[VertexId], ("publishes", "", Constants.defaultLong))
        twitterUserTweetRelation += edge
        nodesArray = nodesArray :+ (nextVertexNum, t)
        nextVertexNum += 1
      }
    }

    logger.info("Tweets created")
    
    return nextVertexNum
  }
  
  /**
   * Method createPhotos
   * @param lastId last id node used
   * Creates Photo
   */
  def createPhotos(lastId : Long) : Long = {
    
    var nextVertexNum = lastId
    //Creating Photos for Flickr Users
    for (fu <- flickrUsers) {
      val rand = scala.util.Random
      val numPhotos = rand.nextInt(Constants.avgNumPhotoPerUser * 2);
      val it = 0
      for (it <- 1 to numPhotos) {
        val p = new PhotoEvent(nextVertexNum, System.currentTimeMillis(), "photo title", "photo-" + fu.id + "-" + it, "photo description", System.currentTimeMillis(), System.currentTimeMillis(), "cannon");
        photos += p;
        val edge = Edge(fu.idNode.asInstanceOf[VertexId], p.idNode.asInstanceOf[VertexId], ("owns", "", Constants.defaultLong))
        flickrUserPhotoRelation += edge;
        nodesArray = nodesArray :+ (nextVertexNum, p)
        nextVertexNum += 1
      }
    }
    logger.info("Photos created")
    
    return nextVertexNum
  }
  
  /**
   * Method createContains
   * Creates relation Contains
   */
  def createContains(){
    
    for (tweet <- tweets) {
      val rand = scala.util.Random
      val numHashtagsThisTweet = rand.nextInt(Constants.avgNumHashtagsPerTweet * 2)
      val i = 0
      for (i <- 1 to numHashtagsThisTweet) {
        val htn = rand.nextInt(hashtags.size)
        val ht = hashtags(htn)
        val edge = Edge(tweet.idNode.asInstanceOf[VertexId], ht.idNode.asInstanceOf[VertexId], ("contains", "", Constants.defaultLong))
        tweetHashtagEdge += edge
      }
    }

    logger.info("Tweets tagged")
  }
  
  /**
   * Method createTags
   * Creates relation Tags
   */
  def createTags(){
    
    for (photo <- photos) {
      val rand = scala.util.Random
      val numHashtagsThisPhoto = rand.nextInt(Constants.avgNumHashtagsPerTweet * 2)
      val it = 0
      for (it <- 1 to numHashtagsThisPhoto) {
        val htn = rand.nextInt(hashtags.size)
        val ht = hashtags(htn);
        val edge = Edge(photo.idNode.asInstanceOf[VertexId], ht.idNode.asInstanceOf[VertexId], ("tags", "", Constants.defaultLong))
        photoHashtagEdge += edge
      }
    }

    logger.info("Photos tagged")
  }
  
  /**
   * Method createLikes
   * Creates relation Likes
   */
  def createLikes(allUsers: ListBuffer[TwitterUserEvent]){
    for (tweet <- tweets) {
      val rand = scala.util.Random
      val numLikesThisTweet = rand.nextInt(Constants.avgNumLikesPerTweet * 2)
      val it = 0
      for (it <- 1 to numLikesThisTweet) {
        val index = rand.nextInt(allUsers.size)
        val tu = allUsers(index)
        val like = Edge(tu.idNode.asInstanceOf[VertexId], tweet.idNode.asInstanceOf[VertexId], ("likes", "", System.currentTimeMillis()))
        likes += like
      }
    }

    logger.info("Likes created")
  }
  
  /**
   * Method createMentions
   * Creates relation Mentions
   */
  def createMentions(allUsers: ListBuffer[TwitterUserEvent]){
    for (tweet <- tweets) {
      val rand = scala.util.Random
      val numMentionsThisTweet = rand.nextInt(Constants.avgNumMentionsInTweet * 2)
      val it = 0
      for (it <- 1 to numMentionsThisTweet) {
        val index = rand.nextInt(allUsers.size)
        val tu = allUsers(index)
        val mention = Edge(tweet.idNode.asInstanceOf[VertexId], tu.idNode.asInstanceOf[VertexId], ("mentions", "", Constants.defaultLong))
        mentions += mention
      }
    }

    logger.info("Mentions created")
  }
  
  /**
   * Method createFavorites
   * Creates relation Favorites
   */
  def createFavorites(){
    for (photo <- photos) {
      val rand = scala.util.Random
      val numLikesThisPhoto = rand.nextInt(Constants.avgNumFavoritesPerPhoto * 2)
      val it = 0
      for (it <- 1 to numLikesThisPhoto) {
        val index = rand.nextInt(flickrUsers.size)
        val fu = flickrUsers(index)
        val fav = Edge(fu.idNode.asInstanceOf[VertexId], photo.idNode.asInstanceOf[VertexId], ("favorites", "", System.currentTimeMillis()))
        favorites += fav
      }
    }

    logger.info("Favorites created")
  }
  
  /**
   * Method createComments
   * Creates relation Comments
   */
  def createComments(){
    for (photo <- photos) {
      val rand = scala.util.Random
      val numCommentsThisPhoto = rand.nextInt(Constants.avgNumCommentsPerPhoto * 2)
      val it = 0
      for (it <- 1 to numCommentsThisPhoto) {
        val index = rand.nextInt(flickrUsers.size)
        val fu = flickrUsers(index)
        val c = Edge(fu.idNode.asInstanceOf[VertexId], photo.idNode.asInstanceOf[VertexId], ("comments", "comment text", System.currentTimeMillis()))
        comments += c
      }
    }
    logger.info("Comments created")
  }

  /**
   * Method clearAll
   * Clears arrays and ListBuffers
   */
  def clearAll() {
    nodesArray.clear()
    inactiveTwitterUsers.clear()
    influencerTwitterUsers.clear()
    activeTwitterUsers.clear()
    flickrUsers.clear()
    photos.clear()
    tweets.clear()
    hashtags.clear()

    //Edges

    flickrUserUserRelation.clear()
    twitterUserUserRelation.clear()
    twitterUserTweetRelation.clear()
    flickrUserPhotoRelation.clear()
    tweetHashtagEdge.clear()
    photoHashtagEdge.clear()
  }

}
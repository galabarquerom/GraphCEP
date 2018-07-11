package com.cor.graphx.test

import scala.collection.mutable.ListBuffer

import org.apache.spark.SparkContext
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.cor.graphx.controller.TwitterFlickrGenerator
import com.cor.graphx.controller.subscriber.HotTopicSubscriber
import com.cor.graphx.controller.subscriber.InfluencerTweetedSubscriber
import com.cor.graphx.controller.subscriber.MisunderstoodSubscriber
import com.cor.graphx.controller.subscriber.NiceTwitterPhotoSubscriber
import com.cor.graphx.controller.subscriber.PopularFlickrPhotoSubscriber
import com.cor.graphx.controller.subscriber.PopularTwitterPhotoSubscriber
import com.cor.graphx.model.event.HotTopic
import com.cor.graphx.model.event.InfluencerTweeted
import com.cor.graphx.model.event.Misunderstood
import com.cor.graphx.model.event.NiceTwitterPhoto
import com.cor.graphx.model.event.PopularFlickrPhoto
import com.cor.graphx.model.event.PopularTwitterPhoto
import com.cor.graphx.model.event.VertexProperty

/**
 * Class TwitterFlickrHandler
 */
class TwitterFlickrTest(sc: SparkContext) {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  //Graph structures
  var graph: Graph[(VertexProperty), (String, String, Long)] = Graph(sc.emptyRDD, sc.emptyRDD)
  var vertices = ListBuffer[(VertexProperty)]()
  var edges = ListBuffer[Edge[(String, String, Long)]]()

  // HotTopic with Hashtag properties and event date
  var verticesHotTopic: RDD[(Long, HotTopic)] = sc.emptyRDD
  // PopularTwitterPhoto with Photo and event date
  var verticesPopularFP: RDD[(Long, PopularFlickrPhoto)] = sc.emptyRDD

  // PopularTwitterPhoto with Photo, Hashtag and event date
  var verticesPopularTP: RDD[((Long, Long), PopularTwitterPhoto)] = sc.emptyRDD

  //NiceTwitterPhoto
  var verticesNiceTP: RDD[((Long, Long), NiceTwitterPhoto)] = sc.emptyRDD

  //Misunderstood
  var verticesMisunderstood: RDD[((Long, Long), Misunderstood)] = sc.emptyRDD

  //InfluencerTweeted
  var verticesInfluencer: RDD[(Long, InfluencerTweeted)] = sc.emptyRDD


  /**
   * Method initService
   * Method to init the Service and the rules with an infinite thread
   */
  def initService() {
    val g = new TwitterFlickrGenerator(sc)
    graph = g.generateGraph()

    hotTopic()
    popularFlickrPhoto()
    popularTwitterPhoto()
    niceTwitterPhoto()
    misunderstood()
    influencerTweeted()

    logger.info(graph.numVertices + " nodes")
    logger.info(graph.numEdges + " edges")
    logger.info(verticesHotTopic.count + " results after HotTopic")
    logger.info(verticesPopularFP.count + " results after PopularFlickrPhoto")
    logger.info(verticesPopularTP.count + " results after PopularTwitterPhoto")
    logger.info(verticesInfluencer.count + " results after InfluencerTweeted")
    logger.info(verticesNiceTP.count + " results after NiceTwitterPhoto")
    logger.info(verticesMisunderstood.count + " results after misunderstood")

  }

  /**
   * Method hotTopic
   * Method to start HotTopic rule
   */
  def hotTopic() {
    val ht = new HotTopicSubscriber(sc)
    val threadHT = new Thread {
      override def run {
        for (it <- 1 to 5) {
          verticesHotTopic = null
          var start = System.currentTimeMillis() //init time
          verticesHotTopic = ht.hotTopic(graph)
          var end = System.currentTimeMillis() //init time
          logger.info((end - start) + " milliseconds for HotTopic")

        }
      }
    }
    threadHT.start
    threadHT.join
  }
  
  /**
   * Method popularTwitterPhoto
   * Method to start PopularTwitterPhoto rule
   */
  def popularTwitterPhoto() {
    val ptp = new PopularTwitterPhotoSubscriber(sc)
    val threadPTP = new Thread {
      override def run {
        for (it <- 1 to 5) {
          verticesPopularTP = null
          var start = System.currentTimeMillis() //init time
          verticesPopularTP = ptp.popularTwitterPhoto(graph)
          var end = System.currentTimeMillis() //init time
          logger.info((end - start) + " milliseconds for PopularTwitterPhoto")
        }
      }
    }
    threadPTP.start
    threadPTP.join
  }
  
  /**
   * Method popularFlickrPhoto
   * Method to start PopularFlickrPhoto rule
   */
  def popularFlickrPhoto() {
    val pfp = new PopularFlickrPhotoSubscriber(sc)
    val threadPFP = new Thread {
      override def run {
        for (it <- 1 to 5) {
          verticesPopularFP = null
          var start = System.currentTimeMillis() //init time
          verticesPopularFP = pfp.popularFlickrPhoto(graph)
          var end = System.currentTimeMillis() //init time
          logger.info((end - start) + " milliseconds for PopularFlickrPhoto")
        }
      }
    }
    threadPFP.start
    threadPFP.join
  }

  /**
   * Method niceTwitterPhoto
   * Method to start NiceTwitterPhoto rule
   */
  def niceTwitterPhoto() {
    val ntp = new NiceTwitterPhotoSubscriber(sc)
    val threadNTP = new Thread {
      override def run {
        for (it <- 1 to 5) {
          verticesNiceTP = null
          var start = System.currentTimeMillis() //init time
          verticesNiceTP = ntp.niceTwitterPhoto(graph)
          var end = System.currentTimeMillis() //init time
          logger.info((end - start) + " milliseconds for NiceTwitterPhoto")
        }
      }
    }
    threadNTP.start
    threadNTP.join

  }

  /**
   * Method misunderstood
   * Method to start Misunderstood rule
   */
  def misunderstood() {
    val mu = new MisunderstoodSubscriber(sc)
    val threadMU = new Thread {
      override def run {

        for (it <- 1 to 5) {
          verticesMisunderstood = null
          var start = System.currentTimeMillis() //init time
          verticesMisunderstood = mu.misunderstood(graph, verticesNiceTP)
          var end = System.currentTimeMillis() //init time
          logger.info((end - start) + " milliseconds for Misunderstood")
        }
      }
    }
    threadMU.start
    threadMU.join
  }
  
  /**
   * Method influencerTweeted
   * Method to start InfluencerTweeted rule
   */
  def influencerTweeted() {
    val int = new InfluencerTweetedSubscriber(sc)
    val threadIT = new Thread {
      override def run {
        for (it <- 1 to 5) {
          verticesInfluencer = null
          var start = System.currentTimeMillis() //init time
          verticesInfluencer = int.influencerTweeted(graph)
          var end = System.currentTimeMillis() //init time
          logger.info((end - start) + " milliseconds for InfluencerTweeted")
        }
      }
    }
    threadIT.start
    threadIT.join
  }
}
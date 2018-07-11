package com.cor.graphx.handler

import org.apache.spark.SparkContext
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.VertexId
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.EdgeTriplet
import org.apache.spark.graphx.EdgeDirection
import com.cor.graphx.util.Utils
import org.apache.spark.graphx.VertexRDD
import org.apache.spark.graphx.PartitionStrategy
import com.cor.graphx.model.event.HotTopic
import com.cor.graphx.controller.subscriber.InfluencerTweetedSubscriber
import com.cor.graphx.model.event.InfluencerTweeted
import com.cor.graphx.model.event.NiceTwitterPhoto
import com.cor.graphx.controller.subscriber.HotTopicSubscriber
import com.cor.graphx.model.event.PopularTwitterPhoto
import com.cor.graphx.controller.subscriber.NiceTwitterPhotoSubscriber
import com.cor.graphx.controller.subscriber.PopularTwitterPhotoSubscriber
import com.cor.graphx.controller.subscriber.MisunderstoodSubscriber
import com.cor.graphx.controller.TwitterFlickrGenerator
import com.cor.graphx.model.event.Misunderstood
import com.cor.graphx.model.event.PopularFlickrPhoto
import com.cor.graphx.controller.subscriber.PopularFlickrPhotoSubscriber
import com.cor.graphx.model.event.VertexProperty
import com.cor.graphx.model.event.TweetEvent
/**
 * Class FlickrTwitterEventHandler
 */
class FlickrTwitterEventHandler(sc: SparkContext) {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  //Main graph
  var graph: Graph[(VertexProperty), (String, String, Long)] = Graph(sc.emptyRDD, sc.emptyRDD)
  var vertices: RDD[(VertexProperty)] = sc.emptyRDD
  var edges: RDD[Edge[(String, String, Long)]] = sc.emptyRDD

  //Queries

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

  class FlickrTwitterEventHandler {

    verticesHotTopic.localCheckpoint()
    verticesPopularFP.localCheckpoint()
    verticesPopularTP.localCheckpoint()
    verticesNiceTP.localCheckpoint()
    verticesMisunderstood.localCheckpoint()
    verticesInfluencer.localCheckpoint()
  }

  /**
   * Method initService
   * @return number of events created
   * Method to init the Service and the rules with an infinite thread
   */
  def initService(): Long = {

    var count = 0L
    val g = new TwitterFlickrGenerator(sc)
    graph = g.generateGraph()

    vertices = graph.vertices.map { case (id, attr) => attr }
    edges = graph.edges
    
    logger.info("Please, wait a moment until system starts.")
    
    count = vertices.count()
    hotTopic()
    popularFlickrPhoto()
    popularTwitterPhoto()
    niceTwitterPhoto()
    misunderstood()
    influencerTweeted()

    return count

  }

  /**
   * Method handleVertex
   * @param event
   * Handle the incoming event.
   */
  def handleVertex(event: VertexProperty) = {

    val init = System.currentTimeMillis()

    vertices = vertices ++ sc.parallelize(Seq(event))
    deleteGraph

    val finalTime = System.currentTimeMillis()

    logger.info(finalTime - init + " milliseconds to create vertex")

  }

  /**
   * Method handleEdge
   * @param edge
   * Handle the incoming edge.
   */
  def handleEdge(edge: Edge[(String, String, Long)]) = {
    val logger: Logger = LoggerFactory.getLogger(this.getClass)

    val init = System.currentTimeMillis()

    edges = edges ++ sc.parallelize(Seq(edge))

    deleteGraph

    val finalTime = System.currentTimeMillis()

    logger.info(finalTime - init + " milliseconds to create edge")

  }

  /**
   * Method deleteGraph
   * Delete nodes and edges from graph
   */
  def deleteGraph = {
    val logger: Logger = LoggerFactory.getLogger(this.getClass)

    graph = Graph(vertices.map(r => (r.idVertex, r)), edges)

    val graphCached = graph.cache

    //Filter
    graph = graphCached.subgraph(vpred = (id, attr) => !attr.isInstanceOf[TweetEvent] || (attr.isInstanceOf[TweetEvent] && (System.currentTimeMillis() - attr.currentTimeStamp) < 3600000), epred = e => (e.dstAttr != null) && (e.srcAttr != null) && (!e.dstAttr.isInstanceOf[TweetEvent] || (e.dstAttr.isInstanceOf[TweetEvent] && (System.currentTimeMillis() - e.dstAttr.currentTimeStamp) < 3600000)) && (!e.srcAttr.isInstanceOf[TweetEvent] || (e.srcAttr.isInstanceOf[TweetEvent] && (System.currentTimeMillis() - e.srcAttr.currentTimeStamp) < 3600000)))
    graphCached.unpersist(blocking = true)

    vertices = graph.vertices.map { case (id, attr) => attr }

    edges = graph.edges
  }

  /**
   * Method hotTopic
   * Method to start HotTopic rule
   */
  def hotTopic() {
    val ht = new HotTopicSubscriber(sc)
    val threadHT = new Thread {
      override def run {
        while (true) {
          verticesHotTopic = null
          var start = System.currentTimeMillis() //init time
          verticesHotTopic = ht.hotTopic(graph)
          var end = System.currentTimeMillis() //init time
          logger.info((end - start) + " milliseconds for HotTopic")

        }
      }
    }
    threadHT.start
  }

  /**
   * Method popularTwitterPhoto
   * Method to start PopularTwitterPhoto rule
   */
  def popularTwitterPhoto() {
    val ptp = new PopularTwitterPhotoSubscriber(sc)
    val threadPTP = new Thread {
      override def run {
        while (true) {
          verticesPopularTP = null
          var start = System.currentTimeMillis() //init time
          verticesPopularTP = ptp.popularTwitterPhoto(graph)
          var end = System.currentTimeMillis() //init time
          logger.info((end - start) + " milliseconds for PopularTwitterPhoto")
        }
      }
    }
    threadPTP.start
  }

  /**
   * Method popularFlickrPhoto
   * Method to start PopularFlickrPhoto rule
   */
  def popularFlickrPhoto() {
    val pfp = new PopularFlickrPhotoSubscriber(sc)
    val threadPFP = new Thread {
      override def run {
        while (true) {
          verticesPopularFP = null
          var start = System.currentTimeMillis() //init time
          verticesPopularFP = pfp.popularFlickrPhoto(graph)
          var end = System.currentTimeMillis() //init time
          logger.info((end - start) + " milliseconds for PopularFlickrPhoto")
        }
      }
    }
    threadPFP.start
  }

  /**
   * Method niceTwitterPhoto
   * Method to start NiceTwitterPhoto rule
   */
  def niceTwitterPhoto() {
    val ntp = new NiceTwitterPhotoSubscriber(sc)
    val threadNTP = new Thread {
      override def run {
        while (true) {
          verticesNiceTP = null
          var start = System.currentTimeMillis() //init time
          verticesNiceTP = ntp.niceTwitterPhoto(graph)
          var end = System.currentTimeMillis() //init time
          logger.info((end - start) + " milliseconds for NiceTwitterPhoto")
        }
      }
    }
    threadNTP.start

  }

  /**
   * Method misunderstood
   * Method to start Misunderstood rule
   */
  def misunderstood() {
    val mu = new MisunderstoodSubscriber(sc)
    val threadMU = new Thread {
      override def run {

        while (true) {
          verticesMisunderstood = null
          var start = System.currentTimeMillis() //init time
          verticesMisunderstood = mu.misunderstood(graph, verticesNiceTP)
          var end = System.currentTimeMillis() //init time
          logger.info((end - start) + " milliseconds for Misunderstood")
        }
      }
    }
    threadMU.start
  }

  /**
   * Method influencerTweeted
   * Method to start InfluencerTweeted rule
   */
  def influencerTweeted() {
    val int = new InfluencerTweetedSubscriber(sc)
    val threadIT = new Thread {
      override def run {
        while (true) {
          verticesInfluencer = null
          var start = System.currentTimeMillis() //init time
          verticesInfluencer = int.influencerTweeted(graph)
          var end = System.currentTimeMillis() //init time
          logger.info((end - start) + " milliseconds for InfluencerTweeted")
        }
      }
    }
    threadIT.start
  }

}
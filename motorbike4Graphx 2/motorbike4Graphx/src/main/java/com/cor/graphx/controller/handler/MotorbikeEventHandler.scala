package com.cor.graphx.controller.handler

import scala.collection.mutable.ListBuffer
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.VertexId
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.cor.graphx.model.event.BlowOutTireEvent
import com.cor.graphx.model.event.CrashEvent
import com.cor.graphx.model.event.DriverLeftSeatEvent
import com.cor.graphx.model.event.MotorbikeEvent
import com.cor.graphx.model.event.OccupantThrownAccidentEvent
import com.cor.graphx.model.event.VertexProperty
import com.cor.graphx.controller.subscriber.DriverLeftSeatSubscriber
import com.cor.graphx.controller.subscriber.CrashSubscriber
import com.cor.graphx.controller.subscriber.BlowOutTireSubscriber
import com.cor.graphx.controller.subscriber.OccupantThrownAccidentSubscriber

/**
 * Class MotorbikeEventHandler 
 * Handle events and edges to store them in the graph
 */
class MotorbikeEventHandler(sc: SparkContext) {

  //Graph structures
  var graph: Graph[(VertexProperty), (String, String, Long)] = Graph(sc.emptyRDD, sc.emptyRDD)
  var vertices = ListBuffer[(VertexProperty)]()
  var edges = ListBuffer[Edge[(String, String, Long)]]()

  var graphCrash: Graph[(VertexProperty), (Integer, String, Long)] = Graph(sc.emptyRDD, sc.emptyRDD)

  var graphBlowOutTire: Graph[(VertexProperty), (Integer, String, Long)] = Graph(sc.emptyRDD, sc.emptyRDD)

  var graphDriverLeftSeat: Graph[(VertexProperty), (Integer, String, Long)] = Graph(sc.emptyRDD, sc.emptyRDD)

  var graphOccupantThrownAccident: Graph[(VertexProperty), (Integer, String, Long)] = Graph(sc.emptyRDD, sc.emptyRDD)

  /**
   * Method handleVertex
   * Handles the incoming event.
   */
  def handleVertex(event: VertexProperty) = {
    val logger: Logger = LoggerFactory.getLogger(this.getClass)

    if (event != null) {
      logger.info(event.toString());
      vertices = vertices :+ event
    }

    vertices = vertices.filter { attr => (System.currentTimeMillis() - attr.currentTimeStamp) < 5000 }

    graph = Graph(sc.parallelize(vertices).zipWithIndex().map(r => (r._2.asInstanceOf[VertexId], r._1)), sc.parallelize(edges))
  }

  /**
   * Method handleEdge
   * Handles the incoming edge.
   */
  def handleEdge(edge: Edge[(String, String, Long)]) = {
    val logger: Logger = LoggerFactory.getLogger(this.getClass)

    if (edge != null) {
      logger.info(edge.toString())
      edges = edges :+ edge
    }

    vertices = vertices.filter { attr => (System.currentTimeMillis() - attr.currentTimeStamp) < 5000 }
    graph = Graph(sc.parallelize(vertices).zipWithIndex().map(r => (r._2.asInstanceOf[VertexId], r._1)), sc.parallelize(edges))

  }

  /**
   * Method initService
   * Method to init the Service and the rules with an infinite thread
   */
  def initService() {

    occupantThrownAccident()
    blowOutTire()
    driverLeftSeat()
    crash()

  }

  /**
   * Method crash
   * Calls to Crash subscriber
   */
  def crash() {

    val c = new CrashSubscriber(sc)

    val threadCrash = new Thread {
      override def run {
        while (true) {
          graphCrash = c.crash(graph)

        }
      }
    }

    threadCrash.start

  }

  /**
   * Method blowOutTire
   * Calls to BlowOutTire subscriber
   */
  def blowOutTire() {

    val bot = new BlowOutTireSubscriber(sc)

    val threadBlowOutTire = new Thread {
      override def run {
        while (true) {
          graphBlowOutTire = bot.blowOutTire(graph)

        }
      }
    }
    threadBlowOutTire.start

  }

  /**
   * Method driverLeftSeat
   * Calls to DriverLeftSeat subscriber
   */
  def driverLeftSeat() {

    val dls = new DriverLeftSeatSubscriber(sc)

    val threadDriverLeftSeat = new Thread {
      override def run {
        while (true) {
          graphDriverLeftSeat = dls.driverLeftSeat(graph)

        }
      }
    }

    threadDriverLeftSeat.start

  }

  /**
   * Method occupantThrownAccident
   * Calls to OccupantThrownAccident subscriber
   */
  def occupantThrownAccident() {
    val ota = new OccupantThrownAccidentSubscriber(sc)

    val threadOccupantThrownAccident = new Thread {
      override def run {
        while (true) {
          graphOccupantThrownAccident = ota.occupantThrownAccident(graphCrash, graphDriverLeftSeat, graphBlowOutTire)

        }
      }
    }

    threadOccupantThrownAccident.start

  }

}
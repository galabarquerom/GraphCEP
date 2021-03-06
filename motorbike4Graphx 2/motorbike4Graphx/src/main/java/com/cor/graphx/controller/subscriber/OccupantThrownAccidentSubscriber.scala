package com.cor.graphx.controller.subscriber

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
import com.cor.graphx.model.event.OccupantThrownAccidentEvent
import com.cor.graphx.model.event.VertexProperty

/**
 * OccupantThrownAccidentSubscriber
 */
class OccupantThrownAccidentSubscriber(sc: SparkContext) {

  //List of events OccupantThrownAccident
  var verticesOccupantThrownAccident = ListBuffer[(VertexProperty)]()

  /**
   * Method occupantThrownAccident
   * Updates verticesOccupantThrownAccident
   * @param graphCrash stores Crash events
   * @param graphDriverLeftSeat stores DriverLeftSeat events
   * @param graphBlowOutTire stores BlowOutTire events
   * @return graph with OccupantThrownAccident events
   */
  def occupantThrownAccident(graphCrash: Graph[(VertexProperty), (Integer, String, Long)], graphDriverLeftSeat: Graph[(VertexProperty), (Integer, String, Long)], graphBlowOutTire: Graph[(VertexProperty), (Integer, String, Long)]): Graph[(VertexProperty), (Integer, String, Long)] = {
    val graphUnion = Graph(graphCrash.vertices.union(graphBlowOutTire.vertices.union(graphDriverLeftSeat.vertices)), graphCrash.edges.union(graphBlowOutTire.edges.union(graphDriverLeftSeat.edges)))
    val groupMotorbike = graphUnion.vertices.groupBy(attr => {
      if (attr._2.isInstanceOf[DriverLeftSeatEvent]) {
        attr._2.asInstanceOf[DriverLeftSeatEvent].motorbikeId
      } else if (attr._2.isInstanceOf[CrashEvent]) {
        attr._2.asInstanceOf[CrashEvent].motorbikeId
      } else if (attr._2.isInstanceOf[BlowOutTireEvent]) {
        attr._2.asInstanceOf[BlowOutTireEvent].motorbikeId
      }
    })

    val groupMotorbikeOrder = groupMotorbike.map(f => (f._1, f._2.toList.sortBy(f => f._2.asInstanceOf[VertexProperty].currentTimeStamp))).collect
    verticesOccupantThrownAccident = verticesOccupantThrownAccident ++ groupMotorbikeOrder.flatMap(f => isOccupantThrownAccident(f._2))
    val occupantThrownAccidentVertices = sc.parallelize(verticesOccupantThrownAccident).zipWithIndex().map(r => (r._2.asInstanceOf[VertexId], r._1))
    verticesOccupantThrownAccident = verticesOccupantThrownAccident.filter { attr => (System.currentTimeMillis() - attr.currentTimeStamp) <= 3000 } //TODO ventana
    val graphOccupantThrownAccident = Graph(occupantThrownAccidentVertices, sc.parallelize(ListBuffer[Edge[(Integer, String, Long)]]()))

    return graphOccupantThrownAccident
  }

  /**
   * Method isOccupantThrownAccident
   * @param list list with DriverLeftSeat, Crash and BlowOutTire events
   * @return list with OccupantThrownAccident events produced
   * Checks if a OccupantThrownAccident event has been produced and return a list with those events
   */
  def isOccupantThrownAccident(list: List[(VertexId, VertexProperty)]): ListBuffer[(VertexProperty)] = {
    val logger: Logger = LoggerFactory.getLogger(this.getClass)
    var result = ListBuffer[(VertexProperty)]()
    for (a <- list; b <- list; c <- list) {
      if (a._2.isInstanceOf[BlowOutTireEvent]
        && b._2.isInstanceOf[CrashEvent]
        && c._2.isInstanceOf[DriverLeftSeatEvent]
        && a._2.asInstanceOf[VertexProperty].currentTimeStamp < b._2.asInstanceOf[VertexProperty].currentTimeStamp
        && b._2.asInstanceOf[VertexProperty].currentTimeStamp < c._2.asInstanceOf[VertexProperty].currentTimeStamp
        && (c._2.asInstanceOf[VertexProperty].currentTimeStamp - a._2.asInstanceOf[VertexProperty].currentTimeStamp) <= 3000
        && sc.parallelize(verticesOccupantThrownAccident).filter(attr => attr.asInstanceOf[OccupantThrownAccidentEvent].currentTimestamp1 == a._2.asInstanceOf[BlowOutTireEvent].timestamp && attr.asInstanceOf[OccupantThrownAccidentEvent].currentTimestamp2 == b._2.asInstanceOf[CrashEvent].timestamp && attr.asInstanceOf[OccupantThrownAccidentEvent].currentTimestamp3 == c._2.asInstanceOf[DriverLeftSeatEvent].timestamp).count == 0) {
        val occupantThrownAccidentEvent = new OccupantThrownAccidentEvent(System.currentTimeMillis(), c._2.asInstanceOf[DriverLeftSeatEvent].motorbikeId, c._2.asInstanceOf[DriverLeftSeatEvent].location, a._2.asInstanceOf[BlowOutTireEvent].timestamp, b._2.asInstanceOf[CrashEvent].timestamp, c._2.asInstanceOf[DriverLeftSeatEvent].timestamp)
        logger.info(occupantThrownAccidentEvent.toString)
        result += occupantThrownAccidentEvent

      }
    }

    return result
  }

}
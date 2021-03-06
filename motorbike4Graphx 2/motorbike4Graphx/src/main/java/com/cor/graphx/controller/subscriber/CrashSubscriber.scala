package com.cor.graphx.controller.subscriber

import com.cor.graphx.model.event.MotorbikeEvent
import com.cor.graphx.model.event.CrashEvent
import org.apache.spark.SparkContext
import scala.collection.mutable.ListBuffer
import com.cor.graphx.model.event.VertexProperty
import org.apache.spark.graphx.Graph
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.apache.spark.graphx.VertexId
import org.apache.spark.graphx.Edge

/**
 * Class CrashSubscriber
 */
class CrashSubscriber(sc: SparkContext) {

  //List of events Crash
  var verticesCrash = ListBuffer[(VertexProperty)]()

  /**
   * Method crash
   * Updates verticesCrash
   * @param graph stores events
   * @return graph with Crash events
   */
  def crash(graph: Graph[(VertexProperty), (String, String, Long)]): Graph[(VertexProperty), (Integer, String, Long)] = {

    val filterSpeed = graph.subgraph(vpred = (id, attr) => attr.isInstanceOf[MotorbikeEvent] && (attr.asInstanceOf[MotorbikeEvent].speed >= 50 || attr.asInstanceOf[MotorbikeEvent].speed == 0) && (System.currentTimeMillis() - attr.currentTimeStamp < 3000))
    val groupMotorbike = filterSpeed.vertices.groupBy(attr => attr._2.asInstanceOf[MotorbikeEvent].motorbikeId)
    val groupMotorbikeOrder = groupMotorbike.map(f => (f._1, f._2.toList.sortBy(f => f._2.asInstanceOf[MotorbikeEvent].currentTimestamp))).collect
    verticesCrash = verticesCrash ++ groupMotorbikeOrder.flatMap(f => isCrash(f._2))
    val crashVertices = sc.parallelize(verticesCrash).zipWithIndex().map(r => (r._2.asInstanceOf[VertexId], r._1))
    verticesCrash = verticesCrash.filter { attr => (System.currentTimeMillis() - attr.currentTimeStamp) < 3000 }

    val graphCrash = Graph(crashVertices, sc.parallelize(ListBuffer[Edge[(Integer, String, Long)]]()))

    return graphCrash

  }

  /**
   * Method isCrash
   * @param list list with motorbike events
   * @return list with Crash events produced
   * Checks if a Crash event has been produced and return a list with those events
   */
  def isCrash(list: List[(VertexId, VertexProperty)]): ListBuffer[(VertexProperty)] = {
    val logger: Logger = LoggerFactory.getLogger(this.getClass)
    var result = ListBuffer[(VertexProperty)]()

    for (a <- list; b <- list) {
      if (a._2.asInstanceOf[MotorbikeEvent].speed >= 50
        && b._2.asInstanceOf[MotorbikeEvent].speed == 0
        && (b._2.asInstanceOf[MotorbikeEvent].currentTimestamp - a._2.asInstanceOf[MotorbikeEvent].currentTimestamp) <= 3000 && (b._2.asInstanceOf[MotorbikeEvent].currentTimestamp >= a._2.asInstanceOf[MotorbikeEvent].currentTimestamp)
        && sc.parallelize(verticesCrash).filter(attr => attr.asInstanceOf[CrashEvent].currentTimestamp1 == a._2.asInstanceOf[MotorbikeEvent].currentTimestamp && attr.asInstanceOf[CrashEvent].currentTimestamp2 == b._2.asInstanceOf[MotorbikeEvent].currentTimestamp).count == 0) {
        val crashEvent = new CrashEvent(System.currentTimeMillis(), a._2.asInstanceOf[MotorbikeEvent].motorbikeId, b._2.asInstanceOf[MotorbikeEvent].location, a._2.asInstanceOf[MotorbikeEvent].speed, b._2.asInstanceOf[MotorbikeEvent].speed, a._2.asInstanceOf[MotorbikeEvent].currentTimestamp, b._2.asInstanceOf[MotorbikeEvent].currentTimestamp)
        result += crashEvent
        logger.info(crashEvent.toString)
      }
    }

    return result
  }

}
package com.cor.graphx.controller.subscriber

import com.cor.graphx.model.event.MotorbikeEvent
import org.apache.spark.SparkContext
import com.cor.graphx.model.event.BlowOutTireEvent
import scala.collection.mutable.ListBuffer
import com.cor.graphx.model.event.VertexProperty
import org.apache.spark.graphx.Graph
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.apache.spark.graphx.VertexId
import org.apache.spark.graphx.Edge

/**
 * Class BlowOutTireSubscriber
 */

class BlowOutTireSubscriber(sc: SparkContext) {

  //List of events BlowOutTire
  var verticesBlowOutTire = ListBuffer[(VertexProperty)]()

  /**
   * Method blowOutTire
   * Updates verticesBlowOutTire
   * @param graph stores events
   * @return graph with BlowOutTire events
   */
  def blowOutTire(graph: Graph[(VertexProperty), (String, String, Long)]): Graph[(VertexProperty), (Integer, String, Long)] = {
    val filterPressure = graph.subgraph(vpred = (id, attr) => attr.isInstanceOf[MotorbikeEvent] && ((attr.asInstanceOf[MotorbikeEvent].tirePressure1 >= 2 || attr.asInstanceOf[MotorbikeEvent].tirePressure1 <= 1.2) || (attr.asInstanceOf[MotorbikeEvent].tirePressure2 >= 2 || attr.asInstanceOf[MotorbikeEvent].tirePressure2 <= 1.2)))
    val groupMotorbike = filterPressure.vertices.groupBy(attr => attr._2.asInstanceOf[MotorbikeEvent].motorbikeId)
    val groupMotorbikeOrder = groupMotorbike.map(f => (f._1, f._2.toList.sortBy(f => f._2.asInstanceOf[MotorbikeEvent].currentTimestamp))).collect
    verticesBlowOutTire = verticesBlowOutTire ++ groupMotorbikeOrder.flatMap(f => isBlowOutTire(f._2))
    val blowOutTireVertices = sc.parallelize(verticesBlowOutTire).zipWithIndex().map(r => (r._2.asInstanceOf[VertexId], r._1))
    verticesBlowOutTire = verticesBlowOutTire.filter { attr => (System.currentTimeMillis() - attr.currentTimeStamp) < 3000 }

    val graphBlowOutTire = Graph(blowOutTireVertices, sc.parallelize(ListBuffer[Edge[(Integer, String, Long)]]()))

    return graphBlowOutTire

  }

  /**
   * Method isBlowOutTire
   * @param list list with motorbike events
   * @return list with BlowOutTire events produced
   * Checks if a BlowOutTire event has been produced and return a list with those events
   */
  def isBlowOutTire(list: List[(VertexId, VertexProperty)]): ListBuffer[(VertexProperty)] = {
    val logger: Logger = LoggerFactory.getLogger(this.getClass)
    var result = ListBuffer[(VertexProperty)]()
    for (a <- list; b <- list) {
      if (a._2.asInstanceOf[MotorbikeEvent].tirePressure1 >= 2
        && b._2.asInstanceOf[MotorbikeEvent].tirePressure1 <= 1.2
        && (b._2.asInstanceOf[MotorbikeEvent].currentTimestamp - a._2.asInstanceOf[MotorbikeEvent].currentTimestamp) < 5000 && (b._2.asInstanceOf[MotorbikeEvent].currentTimestamp > a._2.asInstanceOf[MotorbikeEvent].currentTimestamp)
        && sc.parallelize(verticesBlowOutTire).filter(attr => attr.asInstanceOf[BlowOutTireEvent].currentTimestamp1 == a._2.asInstanceOf[MotorbikeEvent].currentTimestamp && attr.asInstanceOf[BlowOutTireEvent].currentTimestamp2 == b._2.asInstanceOf[MotorbikeEvent].currentTimestamp).count == 0) {
        val blowOutTireEvent = new BlowOutTireEvent(System.currentTimeMillis(), a._2.asInstanceOf[MotorbikeEvent].motorbikeId, b._2.asInstanceOf[MotorbikeEvent].location, a._2.asInstanceOf[MotorbikeEvent].tirePressure1, a._2.asInstanceOf[MotorbikeEvent].tirePressure2, b._2.asInstanceOf[MotorbikeEvent].location, b._2.asInstanceOf[MotorbikeEvent].tirePressure1, b._2.asInstanceOf[MotorbikeEvent].tirePressure2, a._2.asInstanceOf[MotorbikeEvent].currentTimestamp, b._2.asInstanceOf[MotorbikeEvent].currentTimestamp)
        logger.info(blowOutTireEvent.toString)
        result += blowOutTireEvent
      } else {

        if (a._2.asInstanceOf[MotorbikeEvent].tirePressure2 >= 2
          && b._2.asInstanceOf[MotorbikeEvent].tirePressure2 <= 1.2
          && (b._2.asInstanceOf[MotorbikeEvent].currentTimestamp - a._2.asInstanceOf[MotorbikeEvent].currentTimestamp) < 5000 && (b._2.asInstanceOf[MotorbikeEvent].currentTimestamp > a._2.asInstanceOf[MotorbikeEvent].currentTimestamp)
          && sc.parallelize(verticesBlowOutTire).filter(attr => attr.asInstanceOf[BlowOutTireEvent].currentTimestamp1 == a._2.asInstanceOf[MotorbikeEvent].currentTimestamp && attr.asInstanceOf[BlowOutTireEvent].currentTimestamp2 == b._2.asInstanceOf[MotorbikeEvent].currentTimestamp).count == 0) {
          val blowOutTireEvent = new BlowOutTireEvent(System.currentTimeMillis(), a._2.asInstanceOf[MotorbikeEvent].motorbikeId, b._2.asInstanceOf[MotorbikeEvent].location, a._2.asInstanceOf[MotorbikeEvent].tirePressure1, a._2.asInstanceOf[MotorbikeEvent].tirePressure2, b._2.asInstanceOf[MotorbikeEvent].location, b._2.asInstanceOf[MotorbikeEvent].tirePressure1, b._2.asInstanceOf[MotorbikeEvent].tirePressure2, a._2.asInstanceOf[MotorbikeEvent].currentTimestamp, b._2.asInstanceOf[MotorbikeEvent].currentTimestamp)
          logger.info(blowOutTireEvent.toString)
          result += blowOutTireEvent
        }
      }
    }

    return result
  }

}
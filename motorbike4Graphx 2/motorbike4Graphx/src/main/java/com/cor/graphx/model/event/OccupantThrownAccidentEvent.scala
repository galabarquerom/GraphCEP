package com.cor.graphx.model.event

/**
 * Class for event OccupantThrownAccident
 */

case class OccupantThrownAccidentEvent(timestamp: Long, motorbikeId: Integer, location: String, currentTimestamp1: Long, currentTimestamp2: Long, currentTimestamp3: Long) extends VertexProperty(timestamp) {
  /**
   * toString Method
   */
  override def toString(): String = {
    return "OccupantThrownAccidentEvent [" + timestamp + "," + motorbikeId + "," + location + "]";
  }

}

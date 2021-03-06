package com.cor.graphx.model.event

/**
 * Class for event DriverLeftSeat
 */

case class DriverLeftSeatEvent(timestamp: Long, motorbikeId: Integer, location: String, seat_a1: Boolean, seat_a2: Boolean, currentTimestamp1: Long, currentTimestamp2: Long) extends VertexProperty(timestamp) {

  /**
   * toString Method
   */
  override def toString(): String = {
    return "DriverLeftSeatEvent [" + timestamp + "," + motorbikeId + "," + location + "," + seat_a1 + "," + seat_a2 + "]";
  }

}
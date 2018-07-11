package com.cor.graphx.model.event
/**
 * Class for event HotTopic
 */
case class HotTopic(idNode: Long, currentTimestamp: Long) extends VertexProperty(currentTimestamp, idNode) {

  /**
   * toString Method
   */
  override def toString(): String = {
    return "HotTopic [" + currentTimestamp + "]";
  }
}
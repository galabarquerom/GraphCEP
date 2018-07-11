package com.cor.graphx.model.event

/**
 * Class for event Tweet
 */
case class TweetEvent(idNode: Long, currentTimestamp: Long, id: String, text: String, date: Long, source: String) extends VertexProperty(currentTimestamp, idNode) {

  /**
   * toString Method
   */
  override def toString(): String = {
    return "Tweet [" + text + ", " + date + ", " + source + "]";
  }

}
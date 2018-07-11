package com.cor.graphx.model.event

/**
 * Class for event Hashtag
 */
case class HashtagEvent(idNode: Long, currentTimestamp: Long, id: String) extends VertexProperty(currentTimestamp, idNode) {

  /**
   * toString Method
   */
  override def toString(): String = {
    return "Hashtag [" + id + "]";
  }

}
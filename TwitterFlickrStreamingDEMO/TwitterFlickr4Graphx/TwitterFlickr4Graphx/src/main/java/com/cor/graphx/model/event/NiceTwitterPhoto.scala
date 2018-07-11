package com.cor.graphx.model.event

/**
 * Class for event HotTopic
 */
case class NiceTwitterPhoto(idNode: Long, idNodeHashtag: Long, hashtag: String, currentTimestamp: Long) extends VertexProperty(currentTimestamp, idNode) {

  /**
   * toString Method
   */
  override def toString(): String = {
    return "NiceTwitterPhoto [" + hashtag + ", " + currentTimestamp + "]";
  }
}
package com.cor.graphx.model.event

/**
 * Class for event PopularTwitterPhoto
 */
case class PopularTwitterPhoto(idNode: Long, idNodeHashtag: Long, hashtag: String, currentTimestamp: Long) extends VertexProperty(currentTimestamp, idNode) {
  /**
   * toString Method
   */
  override def toString(): String = {
    return "PopularTwitterPhoto [" + hashtag + ", " + currentTimestamp + "]";
  }
}
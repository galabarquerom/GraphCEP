package com.cor.graphx.model.event

/**
 * Class for event Photo
 */
case class PhotoEvent(idNode: Long, currentTimestamp: Long, title: String, id: String, description: String, dateLastUpdate: Long, dateTaken: Long, camera: String) extends VertexProperty(currentTimestamp, idNode) {

  /**
   * toString Method
   */
  override def toString(): String = {
    return "Photo [" + title + ", " + description + ", " + dateLastUpdate + ", " + dateTaken + ", " + camera + "]";
  }

}
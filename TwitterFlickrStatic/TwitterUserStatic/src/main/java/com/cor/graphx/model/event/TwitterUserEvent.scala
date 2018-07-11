package com.cor.graphx.model.event

/**
 * Class for event TwitterUser
 */
case class TwitterUserEvent(idNode: Long, currentTimestamp: Long, id: String, name: String, location: String, verified: String, url: String, tProtected: String, description: String, creationDate: Long) extends VertexProperty(currentTimestamp, idNode) {

  /**
   * toString Method
   */
  override def toString(): String = {
    return "TwitterUser [" + name + "," + location + "," + verified + "," + url + "," + tProtected + "," + description + "," + creationDate + "]";
  }

}
package com.cor.graphx.model.event

/**
 * Class for event FlickrUser
 */
case class FlickrUserEvent(idNode: Long, currentTimestamp: Long, id: String, usrName: String, location: String) extends VertexProperty(currentTimestamp, idNode){
  
  /**
   * toString Method
   */
  override def toString(): String = {
        return "FlickrUser [" + usrName +"," + location + "]";
    }
  
}
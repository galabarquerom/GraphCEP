package com.cor.graphx.model.event

/**
 * Class for event PopularFlickrPhoto
 */
case class PopularFlickrPhoto (idNode: Long, currentTimestamp: Long) extends VertexProperty(currentTimestamp, idNode){
  
  /**
   * toString Method
   */
   override def toString(): String = {
        return "PopularFlickrPhoto [" + currentTimestamp + "]";
    }
}
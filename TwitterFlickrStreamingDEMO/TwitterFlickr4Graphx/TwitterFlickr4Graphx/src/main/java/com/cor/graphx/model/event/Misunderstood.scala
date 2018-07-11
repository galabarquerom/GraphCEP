package com.cor.graphx.model.event

/**
 * Class for event Misunderstood
 */
case class Misunderstood (idNode: Long, idNodePhoto: Long, currentTimestamp: Long) extends VertexProperty(currentTimestamp, idNode){
  
  /**
   * toString Method
   */
   override def toString(): String = {
        return "Misunderstood [" + idNodePhoto + ", " + currentTimestamp + "]";
    }
}
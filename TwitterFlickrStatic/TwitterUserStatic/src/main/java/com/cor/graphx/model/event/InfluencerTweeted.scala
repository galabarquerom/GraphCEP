package com.cor.graphx.model.event

/**
 * Class for event InfluencerTweeted
 */
case class InfluencerTweeted (idNode: Long, currentTimestamp: Long) extends VertexProperty(currentTimestamp, idNode){
  
  /**
   * toString Method
   */
   override def toString(): String = {
        return "InfluencerTweeted [" + currentTimestamp + "]";
    }
  
}
package com.cor.graphx

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import com.cor.graphx.handler.FlickrTwitterEventHandler
import com.cor.graphx.controller.TwitterFlickrController

object main {
  def main(args: Array[String]) {
    
    TwitterFlickrController.start
 
  }
}
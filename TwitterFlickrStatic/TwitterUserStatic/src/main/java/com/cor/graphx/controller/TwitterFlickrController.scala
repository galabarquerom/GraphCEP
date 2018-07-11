package com.cor.graphx.controller

import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import com.cor.graphx.test.TwitterFlickrTest

/**
 * Object TwitterFlickrController
 */
object TwitterFlickrController {

  /**
   * Method start
   * Starts the application
   */
  def start {
    //Logger
    org.apache.log4j.Logger.getLogger("org").setLevel(Level.OFF)
    val logger: org.slf4j.Logger = org.slf4j.LoggerFactory.getLogger(this.getClass)

    //Spark configuration
    val conf = new SparkConf().setAppName("Graph").setMaster("local").set("spark.driver.memory", "2g")
    val sc = new SparkContext(conf)

    logger.info("Please, wait some minutes while graph is created.")

    val handler = new TwitterFlickrTest(sc)

    handler.initService()
  }
}
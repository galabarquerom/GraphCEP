package com.cor.graphx.controller

import scala.io.Source
import java.util.concurrent._
import com.cor.graphx.model.event.MotorbikeEvent
import com.cor.graphx.controller.handler.MotorbikeEventHandler
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.TimerTask
import java.util.Timer
/**
 * Object MotorbikeEventGenerator 
 */
object MotorbikeEventGenerator extends App {

  /**
   *	Method startSendingMotorbikesReadings
   * 	@param motorbikeEventHandler 
   * 	@param events number of events
   *  Creates events and lets the implementation class handle them.
   */
  def startSendingMotorbikesReadings(motorbikeEventHandler: MotorbikeEventHandler, events: Long): Unit = {

    val logger: Logger = LoggerFactory.getLogger(getClass)

    logger.info(getStartingMessage())

    //Utils
    val csvFile: String = "motorbike.csv"
    val cvsSplitBy: String = ";"

    var time_start = 0L
    var time_end = 0L
    var count: Long = 0
    val bufferedSource = Source.fromFile(csvFile)
    val lines = bufferedSource.getLines()

    time_start = System.currentTimeMillis()

    while (count < events) {
      val line = lines.next()
      val motorbike = line.split(cvsSplitBy).map(_.trim)
      val ve = new MotorbikeEvent(motorbike(0).toLong, System.currentTimeMillis(), motorbike(0).toLong, motorbike(1).toInt, motorbike(2), motorbike(3).toDouble, motorbike(4).toDouble, motorbike(5).toDouble, motorbike(6).toBoolean)

      motorbikeEventHandler.handleVertex(ve)

      count = count + 1
    }
    
    time_end = System.currentTimeMillis()

    logger.info("the task has taken " + (time_end - time_start) + " milliseconds");
    logger.info(time_start + " initial time")
    
    //Clean graph
    while (true) { motorbikeEventHandler.handleVertex(null) }
  }

  /**
   * Method startSendingMotorbikesReadings
   * @param motorbikeEventHandler 
   * @param events number of events
   * Creates events and lets the implementation class handle them.
   */
  def startSendingMotorbikesReadingsDelay(motorbikeEventHandler: MotorbikeEventHandler, events: Long): Unit = {

    val logger: Logger = LoggerFactory.getLogger(getClass)

    logger.info(getStartingMessage())

    //Utils
    val csvFile: String = "motorbike.csv";
    val cvsSplitBy: String = ";"

    var time_start = 0L
    var time_end = 0L
    var count: Long = 0

    val bufferedSource = Source.fromFile(csvFile)
    val lines = bufferedSource.getLines()
    time_start = System.currentTimeMillis();

    val updateInterval: Long = 1000

    val timer: Timer = new Timer()

    val timerTask = new TimerTask {

      override def run(): Unit = {
        if (count < events) {
          val line = lines.next()
          val motorbike = line.split(cvsSplitBy).map(_.trim)
          val ve = new MotorbikeEvent(motorbike(0).toLong, System.currentTimeMillis(), motorbike(0).toLong, motorbike(1).toInt, motorbike(2), motorbike(3).toDouble, motorbike(4).toDouble, motorbike(5).toDouble, motorbike(6).toBoolean)

          motorbikeEventHandler.handleVertex(ve)

          count = count + 1
        } else {
          bufferedSource.close
          time_end = System.currentTimeMillis();
          logger.info("the task has taken " + (time_end - time_start) + " milliseconds");
          logger.info(time_start + " initial time")
          timer.cancel()
          while (true) { motorbikeEventHandler.handleVertex(null) }
        }
      }
    }

    timer.scheduleAtFixedRate(timerTask, updateInterval, updateInterval)

  }

  /**
   * Method getStartingMessage
   * Initial message.
   */
  
  def getStartingMessage(): String = {
    val sb = new StringBuilder();
    sb.append("\n\n************************************************************");
    sb.append("\n* STARTING - ");
    sb.append("\n* PLEASE WAIT -");
    sb.append("\n* A WHILE TO SEE WARNING AND CRITICAL EVENTS!");
    sb.append("\n************************************************************\n");
    return sb.toString();
  }
}
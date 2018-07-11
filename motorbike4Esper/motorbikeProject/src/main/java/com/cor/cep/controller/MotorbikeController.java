package com.cor.cep.controller;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.cor.cep.Main;

/**
 * Class MotorbikeController
 */
public class MotorbikeController {

	/** Logger */
	private static Logger LOG = LoggerFactory.getLogger(Main.class);

	/**
	 * Method start 
	 * Starts the application
	 */
	public void start() throws IOException {

		LOG.debug("Starting...");

		// Load spring config
		@SuppressWarnings("resource")
		// Load spring config
		ClassPathXmlApplicationContext appContext = new ClassPathXmlApplicationContext(
				new String[] { "application-context.xml" });
		BeanFactory factory = (BeanFactory) appContext;

		// Selecting properties
		Properties mainProperties = new Properties();

		FileInputStream file;
		String path = "config.properties";
		file = new FileInputStream(path);
		mainProperties.load(file);
		file.close();

		Long nEvents = Long.parseLong(mainProperties.getProperty("NUM_EVENTS"));
		Boolean delay = Boolean.parseBoolean(mainProperties.getProperty("DELAY"));

		MotorbikeEventGenerator generator = (MotorbikeEventGenerator) factory.getBean("eventGenerator");

		// Send Events
		if (delay) {
			// Start Demo with delay
			generator.startSendingMotorbikesReadingsDelay(nEvents);
		} else {
			// Start Demo without delay
			generator.startSendingMotorbikesReadings(nEvents);
		}

	}

}

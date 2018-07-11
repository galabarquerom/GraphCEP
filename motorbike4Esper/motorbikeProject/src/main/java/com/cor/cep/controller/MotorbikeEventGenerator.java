package com.cor.cep.controller;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.cor.cep.controller.handler.MotorbikeEventHandler;
import com.cor.cep.event.MotorbikeEvent;

/**
 * Class MotorbikeEventGenerator Class to read Motorbike events from a .csv file
 * and pass them off to the MotorbikeEventHandler.
 */
@Component
public class MotorbikeEventGenerator {

	/** Logger */
	private static Logger LOG = LoggerFactory.getLogger(MotorbikeEventGenerator.class);

	/**
	 * The MotorbikeEventHandler - wraps the Esper engine and processes the Events
	 */
	@Autowired
	private MotorbikeEventHandler motorbikeEventHandler;

	// Utils
	String csvFile = "motorbike.csv";
	String line = "";
	String cvsSplitBy = ";";

	/**
	 * Class startSendingMotorbikesReadingsDelay
	 * 
	 * @param events
	 *            number of events to test Reads Motorbike events and lets the
	 *            implementation class handle them with a delay of 1 second between
	 *            events.
	 */
	public void startSendingMotorbikesReadingsDelay(final long events) {

		LOG.debug(getStartingMessage());
		final long time_start = System.currentTimeMillis();

		final Timer timer = new Timer();
		try {
			TimerTask task = new TimerTask() {
				int count = 0;

				BufferedReader br = new BufferedReader(new FileReader(csvFile));

				@Override
				public void run() {

					if (count < events) {

						try {
							line = br.readLine();
						} catch (IOException e) {
							e.printStackTrace();
						}

						String[] motorbike = line.split(cvsSplitBy);

						MotorbikeEvent ve = new MotorbikeEvent(Long.parseLong(motorbike[0]),
								Integer.parseInt(motorbike[1]), motorbike[2], Double.parseDouble(motorbike[3]),
								Double.parseDouble(motorbike[4]), Double.parseDouble(motorbike[5]),
								Boolean.valueOf(motorbike[6]));
						motorbikeEventHandler.handle(ve);
						count++;
					} else {
						long time_end = System.currentTimeMillis();
						LOG.info("The task has taken " + (time_end - time_start) + " milliseconds");
						timer.cancel();
					}

				}

			};
			// Timer that introduces the delay
			timer.scheduleAtFixedRate(task, 1000, 1000);

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/**
	 * Class startSendingMotorbikesReadings
	 * 
	 * @param events
	 *            number of events to test Reads Motorbike events and lets the
	 *            implementation class handle them.
	 */
	public void startSendingMotorbikesReadings(final long events) {

		LOG.debug(getStartingMessage());

		ExecutorService xrayExecutor = Executors.newSingleThreadScheduledExecutor();

		xrayExecutor.submit(new Runnable() {
			public void run() {

				LOG.debug(getStartingMessage());

				long time_start, time_end;

				try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
					int count = 0;
					time_start = System.currentTimeMillis();
					while (count < events) {
						line = br.readLine();
						String[] motorbike = line.split(cvsSplitBy);

						MotorbikeEvent ve = new MotorbikeEvent(Long.parseLong(motorbike[0]),
								Integer.parseInt(motorbike[1]), motorbike[2], Double.parseDouble(motorbike[3]),
								Double.parseDouble(motorbike[4]), Double.parseDouble(motorbike[5]),
								Boolean.valueOf(motorbike[6]));
						motorbikeEventHandler.handle(ve);
						count++;
					}
					time_end = System.currentTimeMillis();
					LOG.debug("the task has taken " + (time_end - time_start) + " milliseconds");
				} catch (IOException e) {
					e.printStackTrace();
				}

			}
		});
	}

	/**
	 * Method getStartingMessage 
	 * Initial message.
	 */
	private String getStartingMessage() {
		StringBuilder sb = new StringBuilder();
		sb.append("\n\n************************************************************");
		sb.append("\n* STARTING - ");
		sb.append("\n* PLEASE WAIT -");
		sb.append("\n* A WHILE TO SEE WARNING AND CRITICAL EVENTS!");
		sb.append("\n************************************************************\n");
		return sb.toString();
	}
}

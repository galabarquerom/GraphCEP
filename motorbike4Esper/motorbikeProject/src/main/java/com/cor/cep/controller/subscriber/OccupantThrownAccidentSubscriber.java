package com.cor.cep.controller.subscriber;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Class OccupantThrownAccidentSubscriber
 */
@Component
public class OccupantThrownAccidentSubscriber implements StatementSubscriber {

	/** Logger */
	private static Logger LOG = LoggerFactory.getLogger(OccupantThrownAccidentSubscriber.class);

	/**
	 * {@inheritDoc}
	 */
	public String getStatement() {

		String OccupantThrownAccidentPattern = "@Name('OccupantThrownAccident') "
				+ "insert into OccupantThrownAccident "
				+ "select current_timestamp() as timestamp, a3.motorbikeId as motorbikeId, a3.location as location "
				+ "from pattern [every-distinct(a1.motorbikeId, a1.timestamp) a1 = BlowOutTire -> "
				+ "  (a2 = Crash(a1.motorbikeId = a2.motorbikeId) -> "
				+ "  a3 = DriverLeftSeat(a1.motorbikeId = a3.motorbikeId)) where timer:within(3 seconds)]";

		return OccupantThrownAccidentPattern;
	}

	/**
	 * Method update
	 * 
	 * @param eventMap
	 *            pattern Listener method called when Esper has detected a pattern
	 *            match.
	 */
	public void update(Map<String, Object> eventMap) {

		Long timestamp = (Long) eventMap.get("timestamp");

		Integer motorbikeId = (Integer) eventMap.get("motorbikeId");

		String location = (String) eventMap.get("location");

		StringBuilder sb = new StringBuilder();
		sb.append("***************************************");
		sb.append("\n* [ALERT] : Occupant Thrown Accident EVENT DETECTED! ");
		sb.append("\n* " + timestamp + " , " + motorbikeId + " , " + location);
		sb.append("\n***************************************");

		LOG.debug(sb.toString());
	}

}

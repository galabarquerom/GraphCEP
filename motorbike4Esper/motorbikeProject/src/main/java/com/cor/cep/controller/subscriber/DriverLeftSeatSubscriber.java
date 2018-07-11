package com.cor.cep.controller.subscriber;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Class DriverLeftSeatSubscriber
 */
@Component
public class DriverLeftSeatSubscriber implements StatementSubscriber {

	
	private int count = 0;
    /** Logger */
    private static Logger LOG = LoggerFactory.getLogger(DriverLeftSeatSubscriber.class);

    /**
     * {@inheritDoc}
     */
    public String getStatement() {
    	
	    	String DriverLeftSeatPattern ="@Name('DriverLeftSeat') "
	    			+ "context SegmentedByMotorbikeId "					
	    			+ "insert into DriverLeftSeat "
	    			+ "select current_timestamp() as timestamp, a2.motorbikeId as motorbikeId, a2.location as location, a1.seat as seat_a1, a2.seat as seat_a2 "
	    			+ "from pattern [every a1 = MotorbikeEvent(a1.seat = true) -> a2 = MotorbikeEvent(a2.seat = false)]"; 
	    	    
        return DriverLeftSeatPattern;
    }
    
    /**
     * Method update
     * @param eventMap pattern
     * Listener method called when Esper has detected a pattern match.
     */
    public void update(Map<String, Object> eventMap) {
    	
        Long timestamp = (Long) eventMap.get("timestamp");
        
        Integer motorbikeId = (Integer) eventMap.get("motorbikeId");

        String location = (String) eventMap.get("location");
        
        Boolean seat_a1 = (Boolean) eventMap.get("seat_a1");

        Boolean seat_a2 = (Boolean) eventMap.get("seat_a2");

        StringBuilder sb = new StringBuilder();
        sb.append("***************************************");
        sb.append("\n* [ALERT] : DRIVERLEFTSEAT EVENT DETECTED! ");
        sb.append("\n* " + timestamp + " , " + motorbikeId+ " , " + location + " , " + seat_a1 + " , " + seat_a2);
        sb.append("\n***************************************");
        count++;
        LOG.debug(sb.toString() + Integer.toString(count));
    }

    
}

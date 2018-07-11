package com.cor.cep.controller.subscriber;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Class BlowOutTireSubscriber
 */
@Component
public class BlowOutTireSubscriber implements StatementSubscriber {
	private int count = 0;

    /** Logger */
    private static Logger LOG = LoggerFactory.getLogger(BlowOutTireSubscriber.class);

    /**
     * {@inheritDoc}
     */
    public String getStatement() {
    	
	    	String BlowOutTirePattern =			"@Name('BlowOutTire') "				
	    			+ "context SegmentedByMotorbikeId "				
	    			+ "insert into BlowOutTire "
	    			+ "select current_timestamp() as timestamp, a2.timestamp as timestampTest, a2.motorbikeId as motorbikeId, "
	    		    + "  a1.location as location_a1, "
	    		    + "  a1.tirePressure1 as tirePressure1_a1, "
	    		    + "  a1.tirePressure2 as tirePressure2_a1, "
	    		    + "  a2.location as location_a2, "			    
	    		    + "  a2.tirePressure1 as tirePressure1_a2, "
	    		    + "  a2.tirePressure2 as tirePressure2_a2 "
	    			+ "from pattern [(every a1 = MotorbikeEvent(a1.tirePressure1 >= 2.0) -> "
	    			+ "  a2 = MotorbikeEvent(a2.tirePressure1 <= 1.2) where timer:within(5 seconds)) "
	    			+ "  or "
	    			+ "  (every a1 = MotorbikeEvent(a1.tirePressure2 >= 2.0) -> "
	    			+ "  a2 = MotorbikeEvent(a2.tirePressure2 <= 1.2) where timer:within(5 seconds))]";
	    	    
        return BlowOutTirePattern;
    }
    
    /**
     * Method update
     * @param eventMap pattern
     * Listener method called when Esper has detected a pattern match.
     */
    public void update(Map<String, Object> eventMap) {

        Long timestampTest = (Long) eventMap.get("timestampTest");
        Long timestamp = (Long) eventMap.get("timestamp");
        
        Integer motorbikeId = (Integer) eventMap.get("motorbikeId");

        String location1 = (String) eventMap.get("location_a1");
        
        Double pressure1a1 = (Double) eventMap.get("tirePressure1_a1");

        Double pressure1a2 = (Double) eventMap.get("tirePressure1_a2");
        
        String location2 = (String) eventMap.get("location_a2");
        
        Double pressure2a1 = (Double) eventMap.get("tirePressure2_a1");

        Double pressure2a2 = (Double) eventMap.get("tirePressure2_a2");
        

        StringBuilder sb = new StringBuilder();
        sb.append("***************************************");
        sb.append("\n* [ALERT] : BlowOutTire EVENT DETECTED! ");
        sb.append("\n* " + timestampTest + ", "+ timestamp + " , " + motorbikeId + " , " + location1 + " , " + pressure1a1 + " , " + pressure1a2 + " , "+ location2 + " , " + pressure2a1 + " , " + pressure2a2);
        sb.append("\n***************************************");
        count++;
        LOG.debug(sb.toString() + Integer.toString(count));
    }

}

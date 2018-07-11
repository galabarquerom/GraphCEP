package com.cor.cep.controller.subscriber;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
/** 
 * Class CrashSubscriber
 */
@Component
public class CrashSubscriber implements StatementSubscriber{
	
	private int count = 0;
    /** Logger */
    private static Logger LOG = LoggerFactory.getLogger(CrashSubscriber.class);

    /**
     * {@inheritDoc}
     */
    public String getStatement() {
    	
	    	String CrashPattern ="@Name('Crash') "				
	    			+ "context SegmentedByMotorbikeId "					
	    			+ "insert into Crash "
	    			+ "select current_timestamp() as timestamp, a2.motorbikeId as motorbikeId, "
	    			+ "  a2.location as location, a1.speed as speed_a1, a2.speed as speed_a2 "
	    			+ "from pattern [every a1 = MotorbikeEvent(a1.speed >= 50) -> a2 = MotorbikeEvent(a2.speed = 0) "
	    				+ "where timer:within(3 seconds)]";
	    	    
        return CrashPattern;
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
        
        Double speeda1 = (Double) eventMap.get("speed_a1");

        Double speeda2 = (Double) eventMap.get("speed_a2");
        
        

        StringBuilder sb = new StringBuilder();
        sb.append("***************************************");
        sb.append("\n* [ALERT] : Crash EVENT DETECTED! ");
        sb.append("\n* " + timestamp + " , " + motorbikeId + " , " + location + " , " + speeda1 + " , " + speeda2);
        sb.append("\n***************************************");
        count++;
        LOG.debug(sb.toString() + Integer.toString(count));
    }


}

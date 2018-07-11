package com.cor.cep.controller.handler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.cor.cep.controller.subscriber.StatementSubscriber;
import com.cor.cep.event.MotorbikeEvent;
import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;

/**
 * Class MotorbikeEventHandler
 * This class handles incoming Motorbike Events. It processes them through the EPService, to which
 * it has attached the queries.
 */
@Component
@Scope(value = "singleton")
public class MotorbikeEventHandler implements InitializingBean{

    /** Logger */
    private static Logger LOG = LoggerFactory.getLogger(MotorbikeEventHandler.class);

    /** Esper service */
    private EPServiceProvider epService;
    private EPStatement driverLeftSeatEventStatement;
    private EPStatement blowOutTireEventStatement;
    private EPStatement crashEventStatement;
    private EPStatement occupantThrownAccidentEventStatement;
    
    @Autowired
    @Qualifier("driverLeftSeatSubscriber")
    private StatementSubscriber driverLeftSeat;
    @Autowired
    @Qualifier("blowOutTireSubscriber")
    private StatementSubscriber blowOutTire;
    @Autowired
    @Qualifier("crashSubscriber")
    private StatementSubscriber crash;
    @Autowired
    @Qualifier("occupantThrownAccidentSubscriber")
    private StatementSubscriber occupantThrownAccident;

    /**
     * Method initService
     * Configure Esper Statement(s).
     */
    public void initService() {

        LOG.debug("Initializing Service ..");
        Configuration config = new Configuration();
        config.addEventTypeAutoName("com.cor.cep.event");
        epService = EPServiceProviderManager.getDefaultProvider(config);
        epService.getEPAdministrator().createEPL("create context SegmentedByMotorbikeId partition by motorbikeId from MotorbikeEvent");
        createDriverLeftSeatCheckExpression();
        createBlowOutTireCheckExpression();
        createCrashCheckExpression();
        createOccupantThrownAccidentCheckExpression();

    }

    /**
     * Method createDriverLeftSeatCheckExpression
     * EPL to check DriverLeftSeat
     */
    private void createDriverLeftSeatCheckExpression() {
        
        LOG.debug("create Driver Left Seat Check Expression");
        driverLeftSeatEventStatement = epService.getEPAdministrator().createEPL(driverLeftSeat.getStatement());
        driverLeftSeatEventStatement.setSubscriber(driverLeftSeat);
    }
    
    /**
     * Method createBlowOutTireCheckExpression
     * EPL to check BlowOutTire
     */
    private void createBlowOutTireCheckExpression() {
        
        LOG.debug("create Blow Out Tire Check Expression");
        blowOutTireEventStatement = epService.getEPAdministrator().createEPL(blowOutTire.getStatement());
        blowOutTireEventStatement.setSubscriber(blowOutTire);
    }
    /**
     * Method createCrashCheckExpression
     * EPL to check Crash
     */
    private void createCrashCheckExpression() {
        
        LOG.debug("create Crash Check Expression");
        crashEventStatement = epService.getEPAdministrator().createEPL(crash.getStatement());
        crashEventStatement.setSubscriber(crash);
    }
    
    /**
     * Method createOccupantThrownAccidentCheckExpression
     * EPL to check OccupantThrownAccident
     */
    private void createOccupantThrownAccidentCheckExpression() {
        
        LOG.debug("create Occupant Thrown Accident Check Expression");
        occupantThrownAccidentEventStatement = epService.getEPAdministrator().createEPL(occupantThrownAccident.getStatement());
        occupantThrownAccidentEventStatement.setSubscriber(occupantThrownAccident);
    } 

    /**
     * Method handle
     * @param event motorbike event to handle
     * Handle the incoming MotorbikeEvent.
     */
    public void handle(MotorbikeEvent event) {

        LOG.debug(event.toString());
        epService.getEPRuntime().sendEvent(event);

    }

    /**
     * Method afterPropertiesSet
     * Starts CEP
     */
    @Override
    public void afterPropertiesSet() {
        
        LOG.debug("Configuring..");
        initService();
    }
}

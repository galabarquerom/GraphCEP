package com.cor.cep.controller.subscriber;

/**
 * Class StatementSubscriber
 * Just a convenience interface to let us easily contain the Esper statements with the Subscribers -
 * just for clarity so it's easy to see the statements the subscribers are registered against.
 */
public interface StatementSubscriber {

    /**
     * Get the EPL Stamement the Subscriber will listen to.
     * @return EPL Statement
     */
    public String getStatement();

}

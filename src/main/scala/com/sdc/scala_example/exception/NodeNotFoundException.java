/**
 * NodeNotFoundException.java
 */
package com.sdc.scala_example.exception;


/**
 * @author Simone De Cristofaro
 * Dec 19, 2017
 */
public class NodeNotFoundException extends Exception {

    private static final long serialVersionUID = 1L;

    /**
     * 
     */
    public NodeNotFoundException() {}

    /**
     * @param message
     */
    public NodeNotFoundException(String message) {

        super(message);
    }

    /**
     * @param cause
     */
    public NodeNotFoundException(Throwable cause) {

        super(cause);
    }

    /**
     * @param message
     * @param cause
     */
    public NodeNotFoundException(String message, Throwable cause) {

        super(message, cause);
    }

    /**
     * @param message
     * @param cause
     * @param enableSuppression
     * @param writableStackTrace
     */
    public NodeNotFoundException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {

        super(message, cause, enableSuppression, writableStackTrace);
    }

}

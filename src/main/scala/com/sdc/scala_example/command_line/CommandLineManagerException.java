/**
 * CommandLineManagerException.java
 */
package com.sdc.scala_example.command_line;


/**
 * @author Simone De Cristofaro
 * Nov 13, 2017
 */
public class CommandLineManagerException extends Exception {

    private static final long serialVersionUID = -4811161581221118455L;

    /**
     * 
     */
    public CommandLineManagerException() {}

    /**
     * @param message
     */
    public CommandLineManagerException(String message) {
        super(message);
    }

    /**
     * @param cause
     */
    public CommandLineManagerException(Throwable cause) {
        super(cause);
    }

    /**
     * @param message
     * @param cause
     */
    public CommandLineManagerException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * @param message
     * @param cause
     * @param enableSuppression
     * @param writableStackTrace
     */
    public CommandLineManagerException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

}

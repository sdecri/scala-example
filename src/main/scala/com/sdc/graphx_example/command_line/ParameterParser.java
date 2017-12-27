/**
 * ParameterParser.java
 */
package com.sdc.graphx_example.command_line;

import java.util.function.Function;

/**
 * @author Simone De Cristofaro
 * Nov 9, 2017
 */
public class ParameterParser {


    public static <R> R parse(PARAMETER parameter, String input, Function<String, R> function) 
    throws CommandLineManagerException{
        
        R toReturn = null;
        try {
            toReturn = function.apply(input);    
        }catch (Throwable e) {
            throw new CommandLineManagerException(String.format("Error parsing argument \"%s\" for parameter \"%s\"", input, parameter.getLongOpt()), e);
        }
        
        return toReturn;
        
    }


}

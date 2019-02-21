/**
 * RUN_TYPE.java
 */
package com.sdc.graphx_example.command_line;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author simone.decristofaro
 * Mar 30, 2017
 */
public enum NETWORK_OUTPUT_FORMAT {

    CSV("csv")
    ,JSON("json")
    ;
    
    private String value;

    /**
     * @param value
     */
    private NETWORK_OUTPUT_FORMAT(String value) {
        this.value = value;
    }
    
    /**
     * @return the {@link NETWORK_OUTPUT_FORMAT#value}
     */
    public String getValue() {
    
        return value;
    }


    public static NETWORK_OUTPUT_FORMAT parse(String value) {
        if(value == null) throw new IllegalStateException("Null value passed to the method");
        
        NETWORK_OUTPUT_FORMAT[] values = NETWORK_OUTPUT_FORMAT.values();
        
        for (NETWORK_OUTPUT_FORMAT runType : values) {
            if(value.equals(runType.getValue()))
                return runType;
        }
        
        throw new IllegalStateException(String.format("No valid value provided. Available values: %s", Arrays.toString(getValues().toArray())));

    }
    
    public static List<String> getValues() {
        
        return Arrays.asList(NETWORK_OUTPUT_FORMAT.values()).stream()
        .map(NETWORK_OUTPUT_FORMAT::getValue).collect(Collectors.toList());
        //.toArray(siye -> new String[siye]);
        
    }
    
}

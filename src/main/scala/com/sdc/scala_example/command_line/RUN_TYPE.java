/**
 * RUN_TYPE.java
 */
package com.sdc.scala_example.command_line;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author simone.decristofaro
 * Mar 30, 2017
 */
public enum RUN_TYPE {

    OSM_CONVERTER("osm-converter")
    ,SHORTEST_PATH("shortest-path")
    ;
    
    private String value;

    /**
     * @param value
     */
    private RUN_TYPE(String value) {
        this.value = value;
    }
    
    /**
     * @return the {@link RUN_TYPE#value}
     */
    public String getValue() {
    
        return value;
    }


    public static RUN_TYPE parse(String value) {
        if(value == null) throw new IllegalStateException("Null value passed to the method");
        
        RUN_TYPE[] values = RUN_TYPE.values();
        
        for (RUN_TYPE runType : values) {
            if(value.equals(runType.getValue()))
                return runType;
        }
        
        throw new IllegalStateException(String.format("No valid value provided. Available values: %s", Arrays.toString(getValues().toArray())));

    }
    
    public static List<String> getValues() {
        
        return Arrays.asList(RUN_TYPE.values()).stream()
        .map(RUN_TYPE::getValue).collect(Collectors.toList());
        //.toArray(siye -> new String[siye]);
        
    }
    
}

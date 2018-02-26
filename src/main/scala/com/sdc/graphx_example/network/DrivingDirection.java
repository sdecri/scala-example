/**
 * DrivingDirection.java
 */
package com.sdc.graphx_example.network;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Simone De Cristofaro
 * Feb 26, 2018
 */
public enum DrivingDirection {

    CLOSED(0)
    ,START2END(1)
    ,END2START(2)
    ,BOTH(3)
    ;
    
    private Integer value;

    /**
     * @param value
     */
    private DrivingDirection(Integer value) {
        this.value = value;
    }
    
    /**
     * @return the {@link DrivingDirection#value}
     */
    public Integer getValue() {
    
        return value;
    }


    public static DrivingDirection typeFor(Integer value) {
        if(value == null) throw new IllegalStateException("Null value passed to the method");
        
        DrivingDirection[] values = DrivingDirection.values();
        
        for (DrivingDirection runType : values) {
            if(value.equals(runType.getValue()))
                return runType;
        }
        
        throw new IllegalStateException(String.format("No valid value provided. Available values: %s", Arrays.toString(getValues().toArray())));

    }
    
    public static List<String> getValues() {
        
        return Arrays.asList(DrivingDirection.values()).stream()
        .map(d -> d.getValue().toString()).collect(Collectors.toList());
        //.toArray(siye -> new String[siye]);
        
    }
    
    
}

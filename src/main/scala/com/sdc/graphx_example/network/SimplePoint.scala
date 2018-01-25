package com.sdc.graphx_example.network

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import jdk.nashorn.internal.codegen.types.LongType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.FloatType

case class SimplePoint(lon: Float, lat: Float ) extends Serializable{
    
    override def toString() :String = "%f%s%f"
    .format(lon, SimplePoint.COORDINATES_SEPATARO, lat)

    
}


object SimplePoint {
    
    val LON = "lon"
    val LAT = "lat"
    
    val COORDINATES_SEPATARO = " "
    
    def parse(value :String) : SimplePoint = {
        
        val v = value.split(COORDINATES_SEPATARO)
        
        SimplePoint(v(0).toFloat, v(1).toFloat)
    }
    
    val SCHEMA = StructType(
        List(StructField(LON, FloatType)
                , StructField(LAT, FloatType)
        )        
    )
    
}


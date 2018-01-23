package com.sdc.graphx_example.network

case class SimplePoint(lon: Float, lat: Float ) extends Serializable{
    
    
    
    override def toString() :String = "%f%s%f"
    .format(lon, SimplePoint.COORDINATES_SEPATARO, lat)

    
}


object SimplePoint {
    
    val COORDINATES_SEPATARO = " "
    
    def parse(value :String) : SimplePoint = {
        
        val v = value.split(COORDINATES_SEPATARO)
        
        SimplePoint(v(0).toFloat, v(1).toFloat)
    }
    
    
}


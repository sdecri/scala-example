package com.sdc.scala_example.network

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.IntegerType

class Node extends Serializable {

    private var id: Int = _
    private var lon: Double = _
    private var lat: Double = _

    def this(id :Int, lon : Double, lat : Double) = {
        this()
        this.id = id
        this.lon = lon
        this.lat = lat
    }

    
    
    def getId(): Int = this.id
    def setId(id: Int) = this.id = id
    def getLon(): Double = lon
    def setLon(lon: Double) = this.lon = lon
    def getLat(): Double = lat
    def setLat(lat: Double) = this.lat = lat

    def toRow(): Row = Row(id, lon, lat)
    
    override def hashCode(): Int = id

    override def equals(that: Any) = {
        that match {
            case other: Node => other.id == id && other.lon == lon && other.lat == lat
            case _ => false
        }
    }
    
    override def toString() : String = "ID = %s, LON = %s, LAT = %s".format(id, lon ,lat)

}


object Node {
    
    val SCHEMA = StructType(
        List(
                StructField("id", IntegerType)
                , StructField("lon", DoubleType)
                , StructField("lat", DoubleType)
        )        
    )
    
}


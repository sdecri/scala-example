package com.sdc.scala_example.network

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

class Node {

    private var id: Int = _
    private var lon: Double = _
    private var lat: Double = _

    def this(id :Int, lon : Double, lat : Double)= {
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

    override def hashCode(): Int = id

    override def equals(that: Any) = {
        that match {
            case f: Node => f.id == id && f.lon == lon && f.lat == lat
            case _ => false
        }
    }
    
    override def toString() : String = "ID = %s, LON = %s, LAT = %s".format(id, lon ,lat)
}
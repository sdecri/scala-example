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
import org.apache.spark.sql.types.LongType

class Node extends Serializable {

    private var id: Long = _
    private var lon: Double = _
    private var lat: Double = _

    def this(id :Long, lon : Double, lat : Double) = {
        this()
        this.id = id
        this.lon = lon
        this.lat = lat
    }
    
        def this(id :String, lon : Double, lat : Double) = {
        this(id.toInt, lon, lat)
    }

    
    
    def getId(): Long = this.id
    def setId(id: Long) = this.id = id
    def getLon(): Double = lon
    def setLon(lon: Double) = this.lon = lon
    def getLat(): Double = lat
    def setLat(lat: Double) = this.lat = lat

    def toRow(): Row = Row(id, lat, lon)
    
    override def hashCode(): Int = id.toInt

    def canEqual(a: Any) = a.isInstanceOf[Node]
    override def equals(that: Any) = {
        that match {
            case other: Node => this.canEqual(other) && other.id == id && other.lon == lon && other.lat == lat
            case _ => false
        }
    }
    
    override def toString() : String = "ID = %s, LON = %s, LAT = %s".format(id, lon ,lat)

}


object Node {
    
    val SCHEMA = StructType(
        List(
                StructField("id", LongType)
                , StructField("latitude", DoubleType)
                , StructField("longitude", DoubleType)
        )        
    )
    
    def findById(id :Long, nodes :List[Node]) : Option[Node] = {
        var l = nodes.filter(_.getId() == id)
        return if (l.isEmpty) None else Some(l(0))
    }
    
    def fromRow(row :Row) : Node = 
        new Node(row.getLong(0)
                , row.getDouble(1)
                , row.getDouble(2))
    
}


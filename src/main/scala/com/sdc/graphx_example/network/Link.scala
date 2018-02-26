package com.sdc.graphx_example.network

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import java.io.Serializable
import org.apache.spark.sql.types.FloatType
import breeze.linalg.split
import org.apache.spark.sql.types.LongType
import com.vividsolutions.jts.geom.LineString
import com.sdc.graphx_example.geometry.GeometryUtils
import org.apache.spark.sql.Encoders

/**
 * length in meters and speed in m/s
 */
case class Link(id: Long, tail: Long, head: Long, length: Float, speed: Float
        ,drivingDirection: Integer, points : Array[SimplePoint]) extends Serializable {

    /**
     * @return the link travel time in seconds
     */
    def getTravelTime(): Float = length / (speed)
    
    def toRow = Row(id, tail, head, length, speed, drivingDirection, toLineString.toText())
    
    def toLineString = GeometryUtils.GEOMETRY_FACTORY.createLineString(points.map(p => p.toCoordinate()))
    
    def getDrivingDirectionEnum :DrivingDirection = DrivingDirection.typeFor(this.drivingDirection)
    
    
    def getId(): Long = this.id
    def getTail(): Long = this.tail
    def getHead(): Long = this.head
    def getLength(): Float = this.length
    def getSpeed(): Float = this.speed
    def getDrivingDirection(): Integer = this.drivingDirection
    def getPoints() : Array[SimplePoint] = this.points
    

    override def hashCode(): Int = id.toInt

    def canEqual(a: Any) = a.isInstanceOf[Link]
    override def equals(that: Any) = {
        that match {
            case other: Link => this.canEqual(other) && other.id == id && other.tail == tail &&
                other.head == head &&
                other.length == length && other.speed == speed
            case _ => false
        }
    }

    
    override def toString() :String = "ID = %d, TAIL = %d, HEAD = %d, LENGTH = %f, SPEED = %f, TAVEL_TIME = %f, POINTS = %s"
    .format(id, tail, head, length, speed, getTravelTime, points.mkString(Link.POINT_SEPARATOR))
    
}

object Link {

    val ENCODER = Encoders.product[Link]
    
    val POINT_SEPARATOR = ", "

    val CSV_OPTIONS = Map("header" -> "true")
    
//    val SCHEMA = StructType(List(
//        StructField("id", LongType)
//        , StructField("tail", LongType)
//        , StructField("head", LongType)
//        , StructField("length", FloatType)
//        , StructField("speed", FloatType)
//        , StructField("points", StructType(SimplePoint.SCHEMA.fields)
//        )
//    ))
    
    val SCHEMA_CSV = StructType(List(
        StructField("id", LongType)
        , StructField("tail", LongType)
        , StructField("head", LongType)
        , StructField("length", FloatType)
        , StructField("speed", FloatType)
        , StructField("drivingDirection", IntegerType)
        , StructField("geom", StringType)
        )
    )

    def fromRow(row : Row) : Link = {

        var geom = row.getString(6)
        geom = geom.replace("LINESTRING (", "").replace(")", "")
        
        val points = geom.split(POINT_SEPARATOR)
        .map(s => SimplePoint.parse(s))
        
        Link(row.getLong(0), row.getLong(1), row.getLong(2), row.getFloat(3), row.getFloat(4), row.getInt(5), points)

    }
}


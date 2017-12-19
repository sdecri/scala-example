package com.sdc.scala_example.network

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import java.io.Serializable
import org.apache.spark.sql.types.FloatType

/**
 * length in meters and speed in m/s
 */
case class Link(id: Long, tail: Long, head: Long, length: Float, speed: Float) extends Serializable {

    /**
     * @return the link travel time in seconds
     */
    def getTravelTime(): Float = length / (speed)

    def toRow(): Row = Row(id, tail, head, length, speed)

    def getId(): Long = this.id
    def getTail(): Long = this.tail
    def getHead(): Long = this.head
    def getLength(): Float = this.length
    def getSpeed(): Float = this.speed

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

    
    override def toString() :String = "ID = %d, TAIL = %d, HEAD = %d, LENGTH = %f, SPEED = %f, TAVEL_TIME = %f"
    .format(id, tail, head, length, speed, getTravelTime)
    
}

object Link {

    val SCHEMA = StructType(List(
        StructField("id", StringType)
        , StructField("src", StringType)
        , StructField("dst", StringType)
        , StructField("length", FloatType)
        , StructField("speed", FloatType)))
        
        
        def fromRow(row :Row): Link = 
            Link(row.getLong(0)
                    , row.getLong(1)
                    , row.getLong(2)
                    , row.getFloat(3)
                    , row.getFloat(4)
                    )
}


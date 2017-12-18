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
    def getTravelTime(): Int = Math.round((length / (speed)))

    def toRow(): Row = Row(id.toString(), tail.toString(), head.toString(), length, speed)

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

    
    override def toString() :String = "ID = %d, TAIL = %d, HEAD = %d, LENGTH = %d, SPEED = %d, TAVEL_TIME = %d"
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
            Link(row.getAs[String](1).toLong
                    , row.getAs[String](2).toLong
                    , row.getAs[String](3).toLong
                    , row.getAs[Float](4)
                    , row.getAs[Float](5)
                    )
}


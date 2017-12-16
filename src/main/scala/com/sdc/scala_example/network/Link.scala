package com.sdc.scala_example.network

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import java.io.Serializable

class Link extends Serializable {

    private var id: Long = _
    private var tail: Long = _
    private var head: Long = _
    /**
     * In meters
     */
    private var length: Int = _
    /**
     * in m/s
     */
    private var speed: Int = _

    def this(id: Long, tail: Long, head: Long, length: Int, speed: Int) = {
        this()
        this.id = id
        this.tail = tail
        this.head = head
        this.length = length
        this.speed = speed
    }
    
    def this(id: String, tail: Long, head: Long, length: Int, speed: Int) = {
        this(id.toInt, tail, head, 0, 50)
    }

    def this(id: Long, tail: Long, head: Long) = {
        this(id, tail, head, 0, 50)
    }

    /**
     * @return the link travel time in seconds
     */
    def getTravelTime(): Int = Math.round((length.asInstanceOf[Double] / (speed))).asInstanceOf[Int]

    def toRow(): Row = Row(id.toString(), tail.toString(), head.toString(), length, speed)

    def getId(): Long = this.id
    def setId(id: Long) = this.id = id
    def getTail(): Long = this.tail
    def setTail(tail: Long) = this.tail = tail
    def getHead(): Long = this.head
    def setHead(head: Long) = this.head = head
    def getLength(): Int = this.length
    def setLength(length: Int) = this.length = length
    def getSpeed(): Int = this.speed
    def setSpeed(length: Int) = this.speed = speed

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
        , StructField("length", IntegerType)
        , StructField("speed", IntegerType)))
        
        
        def fromRow(row :Row): Link = 
            new Link(row.getAs[String](1).toLong
                    , row.getAs[String](2).toLong
                    , row.getAs[String](3).toLong
                    , row.getAs[Int](4)
                    , row.getAs[Int](5)
                    )
}


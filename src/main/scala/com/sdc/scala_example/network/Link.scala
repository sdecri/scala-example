package com.sdc.scala_example.network

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType

class Link {

    private var id: Int = _
    private var tail: Node = _
    private var head: Node = _
    /**
     * In meters
     */
    private var length: Int = _
    /**
     * in m/s
     */
    private var speed: Int = _

    def this(id: Int, tail: Node, head: Node, length: Int, speed: Int) = {
        this()
        this.id = id
        this.tail = tail
        this.head = head
        this.length = length
        this.speed = speed
    }
    
    def this(id: String, tail: Node, head: Node, length: Int, speed: Int) = {
        this(id.toInt, tail, head, 0, 50)
    }

    def this(id: Int, tail: Node, head: Node) = {
        this(id, tail, head, 0, 50)
    }

    /**
     * @return the link travel time in seconds
     */
    def getTravelTime(): Int = Math.round((length.asInstanceOf[Double] / (speed))).asInstanceOf[Int]

    def toRow(): Row = Row(id, tail.getId, head.getId, length, speed)

    def getId(): Int = this.id
    def setId(id: Int) = this.id = id
    def getTail(): Node = this.tail
    def setTail(tail: Node) = this.tail = tail
    def getHead(): Node = this.head
    def setHead(head: Node) = this.head = head
    def getLength(): Int = this.length
    def setLength(length: Int) = this.length = length
    def getSpeed(): Int = this.speed
    def setSpeed(length: Int) = this.speed = speed

    override def hashCode(): Int = id

    override def equals(that: Any) = {
        that match {
            case link: Link => link.id == id && link.tail == tail &&
                link.head == head &&
                link.length == length && link.speed == speed
            case _ => false
        }
    }
}

object Link {

    val SCHEMA = StructType(List(
        StructField("id", IntegerType)
        , StructField("src", IntegerType)
        , StructField("dst", IntegerType)
        , StructField("length", IntegerType)
        , StructField("speed", IntegerType)))
}


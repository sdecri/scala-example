package com.sdc.scala_example.network

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType

class Link {
    
    private var id :Int =_
    private var tail :Node =_
    private var head :Node =_
    private var length :Int =_
    
    def this(id :Int, tail :Node, head :Node) = {
        this()
        this.id = id
        this.tail = tail
        this.head = head
    }
    
    def this(id :Int, tail :Node, head :Node, length :Int) = {
        this(id, tail, head)
        this.length = length
    }
    
    def getId(): Int = this.id
    def setId(id: Int) = this.id = id
    def getTail() :Node = this.tail
    def setTail(tail :Node) = this.tail = tail
    def getHead() :Node = this.head
    def setHead(head :Node) = this.head = head
    def getLength() :Int = this.length
    def setLength(length :Int) = this.length = length
    
    def toRow(): Row = Row(id, tail.getId, head.getId, length)
    
    override def hashCode() :Int = id
    
    override def equals(that: Any) = {
        that match {
            case link: Link => link.id == id && link.tail == tail &&
            link.head == head && 
            link.length == length
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
            ))
}


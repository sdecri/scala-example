package com.sdc.scala_example.network

import org.apache.commons.lang3.builder.EqualsBuilder;

class Link {
    
    private var id :Int =_
    private var tail :Node =_
    private var head :Node =_
    private var length :Double =_
    
    def this(id :Int, tail :Node, head :Node) = {
        this()
        this.id = id
        this.tail = tail
        this.head = head
    }
    
    def this(id :Int, tail :Node, head :Node, length :Double) = {
        this(id, tail, head)
        this.length = length
    }
    
    def getId(): Int = this.id
    def setId(id: Int) = this.id = id
    def getTail() :Node = this.tail
    def setTail(tail :Node) = this.tail = tail
    def getHead() :Node = this.head
    def setHead(head :Node) = this.head = head
    def getLength() :Double = this.length
    def setLength(length :Double) = this.length = length
    
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
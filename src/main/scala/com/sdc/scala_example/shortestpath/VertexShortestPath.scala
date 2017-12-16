package com.sdc.scala_example.shortestpath

import org.apache.commons.lang3.builder.HashCodeBuilder;

class VertexShortestPath extends Serializable {

    private var minCost : Double = _
    private var predecessorLink : Long = _

    def this(minCost : Double, predecessorLink : Long) = {
        this()
        this.minCost = minCost
        this.predecessorLink = predecessorLink
    }

    override def toString() : String = "MIN_COST = %f, PREDECESSOR_LINK = %d"
        .format(minCost, predecessorLink)

    override def hashCode : Int = {
        val prime = 31
        var result = 1
        result = prime * result + minCost.toInt;
        result = prime * result + predecessorLink.toInt;
        return result
    }
    
    def canEqual(a: Any) = a.isInstanceOf[VertexShortestPath]
    override def equals(that: Any) = {
        that match {
            case other: VertexShortestPath => this.canEqual(other) && 
            other.minCost == minCost && other.predecessorLink == predecessorLink
            case _ => false
        }
    }

    def getMinCost() = minCost
    def setMinCost(minCost : Double) = this.minCost = minCost
    def getPredecessorLink() = predecessorLink
    def setPredecessorLink(predecessorLink : Long) = this.predecessorLink = predecessorLink
}
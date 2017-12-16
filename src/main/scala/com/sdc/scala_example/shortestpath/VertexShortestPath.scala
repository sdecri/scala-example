package com.sdc.scala_example.shortestpath

class VertexShortestPath extends Serializable {

    //    private var vertex :VD = _
    private var minCost: Double = _
    private var predecessorLink: Long = _

    //    def this(vertex :VD, minCost :Double, predecessorLink :Long) = {
    //        this()
    //        this.vertex = vertex
    //        this.minCost = minCost
    //        this.predecessorLink = predecessorLink
    //    }

    def this(minCost: Double, predecessorLink: Long) = {
        this()
        this.minCost = minCost
        this.predecessorLink = predecessorLink
    }

    //    def getVertex() = vertex
    //    def setVertex(vertex :VD) = this.vertex = vertex
    def getMinCost() = minCost
    def setMinCost(minCost: Double) = this.minCost = minCost
    def getPredecessorLink() = predecessorLink
    def setPredecessorLink(minCost: Long) = this.predecessorLink = predecessorLink
}
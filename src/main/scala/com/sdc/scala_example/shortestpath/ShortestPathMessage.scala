package com.sdc.scala_example.shortestpath

class ShortestPathMessage extends Serializable{
    
    private var costFromSource :Double = _
    private var predecessorLink: Long = _

        def this(costFromSource: Double, predecessorLink: Long) = {
        this()
        this.costFromSource = costFromSource
        this.predecessorLink = predecessorLink
    }

    //    def getVertex() = vertex
    //    def setVertex(vertex :VD) = this.vertex = vertex
    def getCostFromSource() = costFromSource
    def setCostFromSource(costFromSource: Double) = this.costFromSource = costFromSource
    def getPredecessorLink() = predecessorLink
    def setPredecessorLink(predecessorLink: Long) = this.predecessorLink = predecessorLink

    
}
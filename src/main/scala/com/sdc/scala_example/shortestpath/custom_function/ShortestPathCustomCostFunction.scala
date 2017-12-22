package com.sdc.scala_example.shortestpath.custom_function

import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.EdgeTriplet
import org.apache.spark.graphx.Pregel
import org.apache.spark.graphx._
import java.time.Duration
import org.slf4j.LoggerFactory

/**
 * Computes shortest paths to the given set of landmark vertices, returning a graph where each
 * vertex attribute is a map containing the shortest-path distance to each reachable landmark.
 * Currently supports only Graph of [VD, Float], where VD is an arbitrary vertex type.
 */
object ShortestPathCustomCostFunction extends Serializable {
    
    private val LOG = LoggerFactory.getLogger(getClass)
    
    /** Stores a map from the vertex id of a landmark to the distance to that landmark. */
    type SPMap = Map[VertexId, Float]
    // initial and infinity values, use to relax edges
    private val INITIAL = 0.0f
    private val INFINITY = Int.MaxValue.toFloat

    private def makeMap(x: (VertexId, Float)*) = Map(x: _*)

    private def incrementMap(spmap: SPMap, delta: Float): SPMap = {
        spmap.map { case (v, d) => v -> (d + delta) }
    }

    private def addMaps(spmap1: SPMap, spmap2: SPMap): SPMap = {
        (spmap1.keySet ++ spmap2.keySet).map {
            k => k -> math.min(spmap1.getOrElse(k, INFINITY), spmap2.getOrElse(k, INFINITY))
        }.toMap
    }

    // at this point it does not really matter what vertex type is
    def run[VD](graph: Graph[VD, Float], landmarks: Seq[VertexId]): Graph[SPMap, Float] = {

        val start = System.nanoTime()

        val spGraph = graph.mapVertices { (vid, attr) =>
            // initial value for itself is 0.0 as Float
            if (landmarks.contains(vid)) makeMap(vid -> INITIAL) else makeMap()
        }

        val initialMessage = makeMap()

        def vertexProgram(id: VertexId, attr: SPMap, msg: SPMap): SPMap = {
            addMaps(attr, msg)
        }

        def sendMessage(edge: EdgeTriplet[SPMap, Float]): Iterator[(VertexId, SPMap)] = {
            val newAttr = incrementMap(edge.dstAttr, edge.attr)
            if (edge.srcAttr != addMaps(newAttr, edge.srcAttr)) 
                Iterator((edge.srcId, newAttr))
            else 
                Iterator.empty
        }

        val pregel = Pregel(spGraph, initialMessage, activeDirection = EdgeDirection.In)(vertexProgram, sendMessage, addMaps)

        val elapsed = System.nanoTime() - start
        println("Shortest path elapsed time: %s".format(Duration.ofNanos(elapsed)))
        
        pregel
    }
}
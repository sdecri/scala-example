package com.sdc.scala_example.shortestpath

import scala.reflect.ClassTag

import org.apache.spark.graphx._

/**
 * Computes shortest paths from one source vertex to all vertices, returning a graph where each
 * vertex attribute is a map containing the:<br>
 * <ul>
 * <li> shortest-path distance.
 * </ul>
 */
object ShortestPaths extends Serializable {
    
    val INITIAL_COST = Double.PositiveInfinity

    def initVertex() : VertexShortestPath = new VertexShortestPath(INITIAL_COST, -1)

    /**
     * Computes shortest paths to the given set of landmark vertices.
     *
     * @tparam ED the edge attribute type (not used in the computation)
     *
     * @param graph the graph for which to compute the shortest paths
     * @param landmarks the list of landmark vertex ids. Shortest paths will be computed to each
     * landmark.
     *
     * @return a graph where each vertex attribute is a map containing the shortest-path distance to
     * each reachable landmark vertex.
     */
    def run[VD, ED: ClassTag](graph: Graph[VD, ED], source :VertexId)
    : Graph[VertexShortestPath, ED] = {
        val spGraph = graph.mapVertices { (vid, attr) =>
            if (vid == source) 
                new VertexShortestPath(0, -1) 
            else 
                initVertex()
        }

        val initialMessage = initVertex()

        def vertexProgram(id: VertexId, attr: SPMap, msg: SPMap): SPMap = {
            addMaps(attr, msg)
        }

        def sendMessage(edge: EdgeTriplet[SPMap, _]): Iterator[(VertexId, SPMap)] = {
            val newAttr = incrementMap(edge.dstAttr)
            if (edge.srcAttr != addMaps(newAttr, edge.srcAttr)) Iterator((edge.srcId, newAttr))
            else Iterator.empty
        }

        Pregel(spGraph, initialMessage)(vertexProgram, sendMessage, addMaps)
    }
}
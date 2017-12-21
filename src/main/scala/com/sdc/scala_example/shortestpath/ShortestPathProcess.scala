package com.sdc.scala_example.shortestpath

import com.sdc.scala_example.command_line.AppContext
import com.sdc.scala_example.osm.GraphParquetImporter
import org.slf4j.LoggerFactory
import com.sdc.scala_example.network.GeoFunctions
import com.sdc.scala_example.exception.NodeNotFoundException
import org.apache.spark.sql.Row
import com.sdc.scala_example.App
import org.apache.spark.sql.SaveMode
import org.apache.spark.graphx.Graph
import com.sdc.scala_example.network.Node
import com.sdc.scala_example.network.Link
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.MapType
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.StringType
import com.sdc.scala_example.shortestpath.single_source.ShortestPathSingleSourceForward
import com.sdc.scala_example.shortestpath.single_source.ShortestPathSingleSourceForward.COST_FUNCTION
import com.sdc.scala_example.shortestpath.custom_function.ShortestPathCustomCostFunction
import org.apache.spark.graphx.util.GraphGenerators
import scala.util.Random
import org.apache.spark.sql.types.DoubleType


object ShortestPathProcess {
    
    val LOG = LoggerFactory.getLogger(getClass)
    
    val LANDMARK_DISTANCE = "landmarksDistance"
    
    val VERTEX_SHORTEST_PATH_STANDARD_SCHEMA = StructType(
        List(
                StructField("id", LongType)
                , StructField(LANDMARK_DISTANCE, StringType)
        )        
    )       
    
    private case class Context(graph :Graph[Node, Link], sourceId :Long, costFunction :COST_FUNCTION.Value = COST_FUNCTION.DISTANCE)
    
    private def createContext(appContext : AppContext, session : SparkSession): Context = {
        
        val network = GraphParquetImporter.importToNetwork(session, appContext)
        val graph = network.graph
        graph.cache()
        LOG.info("Graph number of vertices: %d".format(graph.vertices.count()))
        LOG.info("Graph number of edges: %d".format(graph.edges.count()))
        val costFunction = COST_FUNCTION.fromValue(appContext.getCostFunction)
        
        val sourceNodeOption = GeoFunctions.getNearestNode(appContext.getSpSource, network.nodesDF, session
                , appContext.getSpNearestDistance, appContext.getSpNearestAttempts, appContext.getSpNearestFactor) 
        
        if(sourceNodeOption.isEmpty)
            throw new NodeNotFoundException("No node found nearest to the specified geographic point: %s".format(appContext.getSpSource))
        
        val sourceId = sourceNodeOption.get.getId
        
        Context(graph, sourceId, costFunction)        
        
    }
    
    def runShortestPathRandomGraph(appContext : AppContext, session : SparkSession){
        
        val graph = GraphGenerators.logNormalGraph(session.sparkContext, appContext.getSpRandomGraphNumVertices)
        if(appContext.getSpGraphRepartition > 0){
            graph.vertices.repartition(appContext.getSpGraphRepartition)
            graph.edges.repartition(appContext.getSpGraphRepartition)
        }
        graph.cache()
        LOG.info("Graph number of vertices: %d".format(graph.vertices.count()))
        LOG.info("Graph number of edges: %d".format(graph.edges.count()))
        
        val sourceId = Random.nextInt(appContext.getSpRandomGraphNumVertices + 1).toLong
        LOG.info("source id = %d".format(sourceId))
        val spResult = ShortestPaths.run(graph, Seq(sourceId))
        val verticesRDD = spResult.vertices
        val verteicesRowRDD = verticesRDD.map(map => {
            Row.fromSeq(Seq(map._1, map._2.mkString(" | ")))
        })
                
        
        val verticesDF = session.createDataFrame(verteicesRowRDD, VERTEX_SHORTEST_PATH_STANDARD_SCHEMA)
        verticesDF.write.mode(SaveMode.Overwrite)
        .option("header", "true")
        .csv(appContext.getOutputDir + App.SHORTEST_PATH_VERTICES_OUTPUT_FILE_NAME)        
    }
    
    def runShortestPathSingleSourceForward(appContext : AppContext, session : SparkSession) = {
        
        val context = createContext(appContext, session)
        
        val spResult = ShortestPathSingleSourceForward.run(context.graph, context.sourceId, context.costFunction)
        val verticesRDD = spResult.vertices
        val verteicesRowRDD = verticesRDD.map(t => {
            Row.fromSeq(Seq(t._1, t._2.getMinCost(), t._2.getPredecessorLink()))
        })
        
        val verticesDF = session.createDataFrame(verteicesRowRDD, ShortestPathSingleSourceForward.VERTEX_SHORTEST_PATH_SCHEMA)
        verticesDF.write.mode(SaveMode.Overwrite)
        .option("header", "true")
        .csv(appContext.getOutputDir + App.SHORTEST_PATH_VERTICES_OUTPUT_FILE_NAME)
    }
    
 
    
    def runShortestPathStandard(appContext : AppContext, session : SparkSession) = {
        val context = createContext(appContext, session)
        
        val spResult = ShortestPaths.run(context.graph, Seq(context.sourceId))
        val verticesRDD = spResult.vertices
        val verteicesRowRDD = verticesRDD.map(map => {
            Row.fromSeq(Seq(map._1, map._2.mkString(" | ")))
        })
        
        session.createDataFrame(verteicesRowRDD, VERTEX_SHORTEST_PATH_STANDARD_SCHEMA)
        .write.mode(SaveMode.Overwrite)
        .option("header", "true")
        .csv(appContext.getOutputDir + App.SHORTEST_PATH_VERTICES_OUTPUT_FILE_NAME)
    }
    
    
    def runShortestPathCustomCostFunction(appContext : AppContext, session : SparkSession) = {
        val context = createContext(appContext, session)
        
        val costFunction = appContext.getCostFunction
        val graph = context.graph
        .mapEdges ( edge => {
            if(costFunction == COST_FUNCTION.DISTANCE)
                edge.attr.length
            else
                edge.attr.getTravelTime
        })
        
        val spResult = ShortestPathCustomCostFunction.run(graph, Seq(context.sourceId))
        val verticesRDD = spResult.vertices
        val verteicesRowRDD = verticesRDD.map(map => {
            Row.fromSeq(Seq(map._1, map._2.mkString(" | ")))
        })
        
        session.createDataFrame(verteicesRowRDD, VERTEX_SHORTEST_PATH_STANDARD_SCHEMA)
        .write.mode(SaveMode.Overwrite)
        .option("header", "true")
        .csv(appContext.getOutputDir + App.SHORTEST_PATH_VERTICES_OUTPUT_FILE_NAME)
    }
    
}
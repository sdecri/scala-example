package com.sdc.scala_example

import com.sdc.scala_example.network.Node
import com.sdc.scala_example.network.Link
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import com.sdc.scala_example.network.Link
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.slf4j.LoggerFactory
import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.Edge
import com.sdc.scala_example.shortestpath.ShortestPathsCustom
import com.sdc.scala_example.command_line.CommandLineManager
import com.sdc.scala_example.command_line.PARAMETER
import com.sdc.scala_example.command_line.RUN_TYPE
import com.sdc.scala_example.osm.OsmParquetConverter
import java.io.File
import com.sdc.scala_example.osm.GraphParquetImporter
import com.sdc.scala_example.shortestpath.ShortestPathsCustom.COST_FUNCTION
import com.sdc.scala_example.command_line.AppContext
import com.sdc.scala_example.shortestpath.VertexShortestPath
import com.sdc.scala_example.geometry.GeometryUtils
import com.vividsolutions.jts.geom.Point
import org.apache.spark.sql.SQLContext
import com.sdc.scala_example.network.GeoFunctions
import com.sdc.scala_example.exception.NodeNotFoundException
import org.apache.spark.sql.SaveMode

/**
 * @author ${user.name}
 */
object App {

    val LOG = LoggerFactory.getLogger(classOf[App])
    val APP_NAME = "scala-graphx-example"
    val SHORTEST_PATH_VERTICES_OUTPUT_FILE_NAME = "shortest-path-vertices.parquet"
    
    
    def main(args : Array[String]) {

        LOG.info("#################################### START ####################################");

        // read data from command line
        val commandLineManager : CommandLineManager = CommandLineManager.newBuilder().withArgs(args).build();
        if (!commandLineManager.hasHelp()) {

            LOG.info(commandLineManager.getCurrentConfigParameterMessage)
            
            var session : SparkSession = null
            try {
                val appContext = commandLineManager.parse()

                session = initSpark(commandLineManager)

                if (appContext.getRunType == RUN_TYPE.OSM_CONVERTER) {

                    OsmParquetConverter.convertToNetwork(session, appContext)

                } else if (appContext.getRunType == RUN_TYPE.SHORTEST_PATH)

                    runShortestPath(appContext, session)

                else
                    LOG.warn("No available run type specified: %s".format(appContext.getRunType))

            } catch {
                case e : Exception => LOG.error("General error running application", e)
                throw e
            } finally {
                if (session != null)
                    session.close()
            }
        }

        LOG.info("#################################### END ####################################");

    }

    private def runShortestPath(appContext : AppContext, session : org.apache.spark.sql.SparkSession) = {
        
        val context = GraphParquetImporter.Context(appContext.getNodesFilePath, appContext.getLinksFilePath)
        val network = GraphParquetImporter.importToNetwork(session, context)
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
        
        val spResult = ShortestPathsCustom.run(graph, sourceId, costFunction)
        val verticesRDD = spResult.vertices
        val verteicesRowRDD = verticesRDD.map(t => {
            Row.fromSeq(Seq(t._1, t._2.getMinCost(), t._2.getPredecessorLink()))
        })
        
        val verticesDF = session.createDataFrame(verteicesRowRDD, ShortestPathsCustom.VERTEX_SHORTEST_PATH_SCHEMA)
        verticesDF.write.mode(SaveMode.Overwrite)
        .option("header", "true")
        .csv(appContext.getOutputDir + SHORTEST_PATH_VERTICES_OUTPUT_FILE_NAME)
    }

    private def initSpark(commandLineManager : CommandLineManager) : SparkSession = {

        var sparkMaster = commandLineManager.getOptionValue(PARAMETER.SPARK_MASTER.getLongOpt())
        var conf : SparkConf = new SparkConf()
        if (sparkMaster != null) {
            conf.setMaster(sparkMaster)
        }
        conf.setAppName(APP_NAME);
        val session = SparkSession.builder().config(conf).getOrCreate()
        session
    }
}

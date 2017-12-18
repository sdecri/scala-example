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

/**
 * @author ${user.name}
 */
object App {

    val LOG = LoggerFactory.getLogger(classOf[App])
    val APP_NAME = "scala-graphx-example"

    def main(args : Array[String]) {

        LOG.info("#################################### START ####################################");

        // read data from command line
        val commandLineManager : CommandLineManager = CommandLineManager.newBuilder().withArgs(args).build();
        if (!commandLineManager.hasHelp()) {

            var session : SparkSession = null
            try {
                val appContext = commandLineManager.parse()

                session = initSpark(commandLineManager)

                if (appContext.getRunType == RUN_TYPE.OSM_CONVERTER) {

                    val context = OsmParquetConverter.Context(new File(appContext.getOsmNodesFilePath)
                    , new File(appContext.getOsmWaysFilePath), appContext.getOutputDir
                    , appContext.getNodesRepartitionOutput, appContext.getLinksRepartitionOutput)
                    OsmParquetConverter.convertToNetwork(session, context)

                } else if (appContext.getRunType == RUN_TYPE.SHORTEST_PATH)

                    runShortestPath(appContext, session)

                else
                    LOG.warn("No available run type specified: %s".format(appContext.getRunType))

            } catch {
                case e : Exception => LOG.error("General error running application", e)
            } finally {
                if (session != null)
                    session.close()
            }
        }

        LOG.info("#################################### END ####################################");

    }

    def runShortestPath(appContext : AppContext, session : org.apache.spark.sql.SparkSession) = {
        val context = GraphParquetImporter.Context(new File(appContext.getNodesFilePath), new File(appContext.getLinksFilePath))
        val graph = GraphParquetImporter.importToNetwork(session, context)
        graph.cache()
        LOG.info("Graph number of vertices: %d".format(graph.vertices.count()))
        LOG.info("Graph number of edges: %d".format(graph.edges.count()))
        val costFunction = COST_FUNCTION.fromValue(appContext.getCostFunction)
        val spResult = ShortestPathsCustom.run(graph, 101179103, costFunction)
        println(spResult.vertices.collect().take(10).mkString(System.lineSeparator()))

    }

    def initSpark(commandLineManager : com.sdc.scala_example.command_line.CommandLineManager) : SparkSession = {

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

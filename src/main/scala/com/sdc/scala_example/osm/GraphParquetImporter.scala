package com.sdc.scala_example.osm

import java.io.File
import java.io.File
import org.apache.spark.graphx.Graph
import com.sdc.scala_example.network.Link
import org.apache.spark.sql.SparkSession
import com.sdc.scala_example.network.Node
import org.apache.spark.graphx.Edge
import org.slf4j.LoggerFactory
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import com.sdc.scala_example.command_line.AppContext

object GraphParquetImporter {

    private val LOG = LoggerFactory.getLogger(getClass)
    
    case class ImportResult(graph :Graph[Node, Link], nodesDF :Dataset[Row], linksDF :Dataset[Row])
    
    def importToNetwork(sparkSession :SparkSession, appContext :AppContext) :ImportResult = {
       
        LOG.info("Import internal formal network parquet with: %s, %s"
                .format(appContext.getNodesFilePath, appContext.getLinksFilePath))
        
        var nodesDF = sparkSession.read.parquet(appContext.getNodesFilePath)    
        var linksDF = sparkSession.read.parquet(appContext.getLinksFilePath)
        if(appContext.getSpGraphRepartition > 0){
            LOG.info("Repartition graph vertices and edges with num partition = %d".format(appContext.getSpGraphRepartition))
            nodesDF = nodesDF.repartition(appContext.getSpGraphRepartition)
            linksDF = linksDF.repartition(appContext.getSpGraphRepartition)
        }
        
        val nodesRDD = nodesDF.rdd.map(row => (row.getLong(0), Node.fromRow(row)))
        val edgesRDD = linksDF.rdd.map(row => {
            val link = Link.fromRow(row)
            Edge(link.getTail(),link.getHead(), link)
        })

        
        val graph = Graph(nodesRDD, edgesRDD)
        ImportResult(graph, nodesDF, linksDF)
    }
    
    
}
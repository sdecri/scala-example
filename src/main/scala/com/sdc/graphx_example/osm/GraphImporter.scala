package com.sdc.graphx_example.osm

import java.io.File
import java.io.File
import org.apache.spark.graphx.Graph
import com.sdc.graphx_example.network.Link
import org.apache.spark.sql.SparkSession
import com.sdc.graphx_example.network.Node
import org.apache.spark.graphx.Edge
import org.slf4j.LoggerFactory
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import com.sdc.graphx_example.command_line.AppContext
import org.apache.spark.sql.Encoders

object GraphImporter {

    private val LOG = LoggerFactory.getLogger(getClass)
    
    case class ImportResult(graph :Graph[Node, Link], nodesDF :Dataset[Node], linksDF :Dataset[Link])
    
    def importToNetwork(sparkSession :SparkSession, appContext :AppContext) :ImportResult = {
        
        import sparkSession.sqlContext.implicits._
        
        LOG.info("Import internal formal network parquet with: %s, %s"
                .format(appContext.getNodesFilePath, appContext.getLinksFilePath))
        
        var nodesDS = sparkSession.read.schema(Encoders.product[Node].schema).json(appContext.getNodesFilePath).as[Node]
        var linksDS = sparkSession.read.schema(Encoders.product[Link].schema).json(appContext.getLinksFilePath).as[Link]
        if(appContext.getSpGraphRepartition > 0){
            LOG.info("Repartition graph vertices and edges with num partition = %d".format(appContext.getSpGraphRepartition))
            nodesDS = nodesDS.repartition(appContext.getSpGraphRepartition)
            linksDS = linksDS.repartition(appContext.getSpGraphRepartition)
        }
        
        val nodesRDD = nodesDS.rdd.map(node => (node.getId, node))
        val edgesRDD = linksDS.rdd.map(link => {
            Edge(link.getTail(),link.getHead(), link)
        })

        
        val graph = Graph(nodesRDD, edgesRDD)
        ImportResult(graph, nodesDS, linksDS)
    }
    
    
}
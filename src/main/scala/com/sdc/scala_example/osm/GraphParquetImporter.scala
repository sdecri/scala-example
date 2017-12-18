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

object GraphParquetImporter {

    private val LOG = LoggerFactory.getLogger(getClass)
    
    case class Context(nodesFile : File, linksFile : File){
        
        override def toString() :String = "NODES_FILE = %s, LINKS_FILE = %s"
        .format(nodesFile,linksFile)
    }
    
    case class ImportResult(graph :Graph[Node, Link], nodesDF :Dataset[Row], linksDF :Dataset[Row])
    
    def importToNetwork(sparkSession :SparkSession, context :Context) :ImportResult = {
       
        LOG.info("Import internal formal network parquet with context: %s".format(context))
        
        val nodesDF = sparkSession.read.parquet(context.nodesFile.getAbsolutePath)       
        val linksDF = sparkSession.read.parquet(context.linksFile.getAbsolutePath)
        
        val nodesRDD = nodesDF.rdd.map(row => (row.getLong(0), Node.fromRow(row)))
        val edgesRDD = linksDF.rdd.map(row => {
            val link = Link.fromRow(row)
            Edge(link.getTail(),link.getHead(), link)
        })

        
        val graph = Graph(nodesRDD, edgesRDD)
        ImportResult(graph, nodesDF, linksDF)
    }
    
    
}
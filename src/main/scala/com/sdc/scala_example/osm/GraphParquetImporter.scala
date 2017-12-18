package com.sdc.scala_example.osm

import java.io.File
import java.io.File
import org.apache.spark.graphx.Graph
import com.sdc.scala_example.network.Link
import org.apache.spark.sql.SparkSession
import com.sdc.scala_example.network.Node
import org.apache.spark.graphx.Edge

object GraphParquetImporter {
    
    def importToNetwork(sparkSession :SparkSession, nodesFile :File, linksFile :File) : Graph[Node, Link] = {
        
        val nodesDF = sparkSession.read.parquet(nodesFile.getAbsolutePath)       
        val linksDF = sparkSession.read.parquet(linksFile.getAbsolutePath)
        
        nodesDF.printSchema()
        nodesDF.show()
        linksDF.printSchema()
        linksDF.show()
        
        val nodesRDD = nodesDF.rdd.map(row => (row.getLong(0), Node.fromRow(row)))
        val edgesRDD = linksDF.rdd.map(row => {
            val link = Link.fromRow(row)
            Edge(link.getTail(),link.getHead(), link)
        })

        
        val graph = Graph(nodesRDD, edgesRDD)
        graph
    }
    
    
}
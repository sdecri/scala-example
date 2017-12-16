package com.sdc.scala_example.osm

import java.io.File
import java.io.File
import org.apache.spark.graphx.Graph
import com.sdc.scala_example.network.Link
import org.apache.spark.sql.SparkSession

object OsmParquetImporter {
    
    def importParquet[VD](sparkSession :SparkSession, nodesFile :File, waysFile :File) : Graph[VD, Link] = {
        
        val nodesDF = sparkSession.read.parquet(nodesFile.getAbsolutePath)
        nodesDF.printSchema()
        nodesDF.show()
        
        val waysDF = sparkSession.read.parquet(waysFile.getAbsolutePath)
        waysDF.printSchema()
        waysDF.show()
        
        return null
    }
    
    
}
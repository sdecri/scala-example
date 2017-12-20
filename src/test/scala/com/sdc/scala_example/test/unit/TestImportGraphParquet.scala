package com.sdc.scala_example.test.unit

import org.junit.Test
import com.sdc.scala_example.osm.GraphParquetImporter
import java.io.File
import org.hamcrest.Matchers._
import org.junit.Assert._

@Test
class TestImportGraphParquet extends TestWithSparkSession {
    
    @Test
    def testImportGraph() = {
        
        val fileResourceNodes = "/networks/internal/casal-bertone/nodes/";        
        val fileUrlNodes = getClass().getResource(fileResourceNodes);
        val nodesFile = fileUrlNodes.getFile();

        
        val fileResourceLinks = "/networks/internal/casal-bertone/links/";        
        val fileUrlLinks = getClass().getResource(fileResourceLinks);
        val linksFile = fileUrlLinks.getFile();
        
        val context = GraphParquetImporter.Context(nodesFile, linksFile)
        
        val network = GraphParquetImporter.importToNetwork(getSpark(), context)
        val graph = network.graph
        
        assertTrue(graph.vertices.count() > 0)
        println(graph.vertices.collect().take(10).mkString(System.lineSeparator()))
        
        assertTrue(graph.edges.count() > 0)
        println(graph.edges.collect().take(10).mkString(System.lineSeparator()))
        
        network.nodesDF.orderBy("id").show(20)
        
    }
    
}
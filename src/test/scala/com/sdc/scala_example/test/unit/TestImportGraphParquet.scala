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
        val nodesFile = new File(fileUrlNodes.getFile());

        
        val fileResourceWays = "/networks/internal/casal-bertone/links/";        
        val fileUrlWays = getClass().getResource(fileResourceWays);
        val waysFile = new File(fileUrlWays.getFile());
        
        val context = GraphParquetImporter.Context(nodesFile, waysFile)
        
        val graph = GraphParquetImporter.importToNetwork(getSpark(), context)
        
        assertTrue(graph.vertices.count() > 0)
        println(graph.vertices.collect().take(10).mkString(System.lineSeparator()))
        
        assertTrue(graph.edges.count() > 0)
        println(graph.edges.collect().take(10).mkString(System.lineSeparator()))
        
    }
    
}
package com.sdc.graphx_example.test.unit

import org.junit.Test
import com.sdc.graphx_example.osm.GraphParquetImporter
import java.io.File
import org.hamcrest.Matchers._
import org.junit.Assert._
import com.sdc.graphx_example.command_line.AppContext

import org.junit.runner.RunWith
import org.junit.runners.BlockJUnit4ClassRunner

@RunWith(classOf[BlockJUnit4ClassRunner])
class TestImportGraphParquet extends TestWithSparkSession {
    
    @Test
    def testImportGraph() = {
        
        val fileResourceNodes = "/networks/internal/casal-bertone/nodes/";        
        val fileUrlNodes = getClass().getResource(fileResourceNodes);
        val nodesFile = fileUrlNodes.getFile();

        
        val fileResourceLinks = "/networks/internal/casal-bertone/links/";        
        val fileUrlLinks = getClass().getResource(fileResourceLinks);
        val linksFile = fileUrlLinks.getFile();
        
        val appContext = new AppContext
        appContext.setNodesFilePath(nodesFile)
        appContext.setLinksFilePath(linksFile)
        appContext.setSpGraphRepartition(-1)
        
        val network = GraphParquetImporter.importToNetwork(getSpark(), appContext)
        val graph = network.graph
        
        assertTrue(graph.vertices.count() > 0)
        println(graph.vertices.collect().take(10).mkString(System.lineSeparator()))
        
        assertTrue(graph.edges.count() > 0)
        println(graph.edges.collect().take(10).mkString(System.lineSeparator()))
        
        network.nodesDF.orderBy("id").show(20)
        
    }
    
}
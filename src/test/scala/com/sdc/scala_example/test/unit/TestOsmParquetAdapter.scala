package com.sdc.scala_example.test.unit

import org.junit.Test
import com.sdc.scala_example.osm.OsmParquetAdapter
import java.io.File
import org.hamcrest.Matchers._
import org.junit.Assert._
import java.lang.Long

@Test
class TestOsmParquetAdapter extends TestWithSparkSession{

    @Test
    def testConvertToNetwork() = {
        
        val fileResourceNodes = "/networks/casal-bertone/casal-bertone-node.parquet";        
        val fileUrlNodes = getClass().getResource(fileResourceNodes);
        val nodesFile = new File(fileUrlNodes.getFile());

        
        val fileResourceWays = "/networks/casal-bertone/casal-bertone-way.parquet";        
        val fileUrlWays = getClass().getResource(fileResourceWays);
        val waysFile = new File(fileUrlWays.getFile());

        val outputDir :String = "target/test/osm_parquet_adapter/output/testConvertToNetwork/"
        
        val linksRepartition = 3
        val context = OsmParquetAdapter.Context(nodesFile, waysFile, outputDir, linksRepartition = linksRepartition)
        
        OsmParquetAdapter.convertToNetwork(getSpark(), context)
        
        val expectedNodesFile = new File(outputDir + "nodes")
        assertTrue(expectedNodesFile.exists())        
        assertTrue(expectedNodesFile.isDirectory())
        
        val nodeDF = getSpark().read.parquet(expectedNodesFile.getAbsolutePath)
        nodeDF.show()
        assertTrue(nodeDF.count() > 0)
        
        val expectedLinksFile = new File(outputDir + "links")
        assertTrue(expectedLinksFile.exists())
        assertTrue(expectedLinksFile.isDirectory())

        val linkParquetFiles = expectedLinksFile.listFiles() 
        val outputFiles = linkParquetFiles.filter(f => f.getPath().endsWith(".parquet"));
        assertEquals(linksRepartition, outputFiles.length);

        
        val linksDF = getSpark().read.parquet(expectedLinksFile.getAbsolutePath)
        linksDF.show()
        assertTrue(linksDF.count() > 0)

    }

}
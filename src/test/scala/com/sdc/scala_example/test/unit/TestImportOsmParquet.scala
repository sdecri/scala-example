package com.sdc.scala_example.test.unit

import org.junit.Test
import com.sdc.scala_example.osm.OsmParquetImporter
import java.io.File

@Test
class TestImportOsmParquet extends TestWithSparkSession {
    
    @Test
    def testImport() :Unit = {
        
        val fileResourceNodes = "/networks/casal-bertone/casal-bertone-node.parquet";        
        val fileUrlNodes = getClass().getResource(fileResourceNodes);
        val nodesFile = new File(fileUrlNodes.getFile());

        
        val fileResourceWays = "/networks/casal-bertone/casal-bertone-way.parquet";        
        val fileUrlWays = getClass().getResource(fileResourceWays);
        val waysFile = new File(fileUrlWays.getFile());
        
        OsmParquetImporter.importParquet(getSpark(), nodesFile, waysFile)
        
        
    }
    
}
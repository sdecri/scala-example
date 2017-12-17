package com.sdc.scala_example.test.unit

import org.junit.Test
import com.sdc.scala_example.osm.OsmParquetAdapter
import java.io.File
import org.hamcrest.Matchers._
import org.junit.Assert._

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
        OsmParquetAdapter.convertToNetwork(getSpark(), nodesFile, waysFile, outputDir)
        
        val expected = new File(outputDir)
        assertThat(expected.exists(), is(true))        
                
                
    }

}
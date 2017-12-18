package com.sdc.scala_example.test.integration

import org.junit.Test
import com.sdc.scala_example.test.unit.TestWithSparkSession
import com.sdc.scala_example.App
import java.io.File

import org.hamcrest.Matchers._
import org.junit.Assert._
import com.sdc.scala_example.command_line.RUN_TYPE

@Test
class TestOsmConverterProcess extends TestWithSparkSession {

    @Test
    def testProcess() = {

        // clean output directory
        val outputDir : String = "target/test/integration/osm_converter/testProcess/"
        deleteDirectory(outputDir)
        val outputDirFile = new File(outputDir)
        println("Output dir clean? %s".format(!outputDirFile.exists()))

        val fileResourceNodes = "/networks/osm/casal-bertone/casal-bertone-node.parquet";
        val fileUrlNodes = getClass().getResource(fileResourceNodes);

        val fileResourceWays = "/networks/osm/casal-bertone/casal-bertone-way.parquet";
        val fileUrlWays = getClass().getResource(fileResourceWays);

        val linksRepartition = 3

        val args = "--spark-master local --run-type %s --osm-nodes-file %s --osm-ways-file %s --links-repartition-output %d --output-dir %s"
            .format(RUN_TYPE.OSM_CONVERTER.getValue, fileUrlNodes.getFile, fileUrlWays.getFile, linksRepartition, outputDir)

        App.main(args.split(" "))
        
        //here the spark session has been closed
        getOrCreateSparkSession()

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
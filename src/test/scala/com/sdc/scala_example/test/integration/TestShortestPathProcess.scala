package com.sdc.scala_example.test.integration

import org.junit.Test
import com.sdc.scala_example.test.unit.TestWithSparkSession
import com.sdc.scala_example.command_line.RUN_TYPE
import com.sdc.scala_example.shortestpath.ShortestPathsCustom
import java.io.File
import com.sdc.scala_example.App

import org.hamcrest.Matchers._
import org.junit.Assert._
import org.apache.spark.sql.SQLContext
import org.slf4j.LoggerFactory

@Test
class TestShortestPathProcess extends TestWithSparkSession {

    private val LOG = LoggerFactory.getLogger(getClass)
    
    @Test
    def testProcess() = {
        
        val sqlContext = new SQLContext(getSpark().sparkContext)
        import sqlContext.implicits._

        // clean output directory
        val outputDir : String = "target/test/integration/shortest_path_process/testProcess/"
        deleteDirectory(outputDir)
        val outputDirFile = new File(outputDir)
        println("Output dir clean? %s".format(!outputDirFile.exists()))

        
        val fileResourceNodes = "/networks/internal/casal-bertone/nodes/";
        val fileUrlNodes = this.getClass().getResource(fileResourceNodes);

        val fileResourceLinks = "/networks/internal/casal-bertone/links/";
        val fileUrlLinks = this.getClass().getResource(fileResourceLinks);
        

        val args = ("--spark-master local --run-type %s --nodes-file %s --links-file %s" + 
        " --sp-source-lon %f --sp-source-lat %f " + 
        " --sp-nearest-distance %d --sp-cost-function %s --output-dir %s")
            .format(RUN_TYPE.SHORTEST_PATH.getValue, fileUrlNodes.getFile, fileUrlLinks.getFile
                    , 12.53685, 41.89721
                    , 100
                    , ShortestPathsCustom.COST_FUNCTION.DISTANCE.toString()
                    , outputDir)

        App.main(args.split(" "))
        
        //here the spark session has been closed
        getOrCreateSparkSession()

        val expectedVerticesFile = new File(outputDir + App.SHORTEST_PATH_VERTICES_OUTPUT_FILE_NAME)
        assertTrue(expectedVerticesFile.exists())
        assertTrue(expectedVerticesFile.isDirectory())

        val verticesDF = getSpark().read.parquet(expectedVerticesFile.getAbsolutePath)
        verticesDF.cache
        
        assertTrue(verticesDF.count() > 0)
        
        val vertexVisitedDF = verticesDF.select("*").where($"minCost" < ShortestPathsCustom.INITIAL_COST && $"predecessor" != -1)
        vertexVisitedDF.cache
        
        vertexVisitedDF.show()
        
        val visitedVertexCount = vertexVisitedDF.count()
        assertTrue(visitedVertexCount > 0)
        
        LOG.info("Number of visited nodes: %d".format(visitedVertexCount))
        
        val sourceList = verticesDF.select("*").where($"predecessor" === -1 && $"minCost" === 0.0).collect()
        
        assertThat(sourceList.length, is(equalTo(1)))
        
        val source = sourceList(0)
        val expectedSourceId = 296057855l
        assertThat(source.getLong(0), is(equalTo(expectedSourceId)))
        
        
        
    }
    
}
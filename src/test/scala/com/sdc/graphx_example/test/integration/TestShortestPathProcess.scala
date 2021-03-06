package com.sdc.graphx_example.test.integration

import org.junit.Test
import com.sdc.graphx_example.network._
import org.hamcrest.Matchers._
import org.junit.Assert._
import org.junit.runner.RunWith
import org.junit.runners.BlockJUnit4ClassRunner
import org.slf4j.LoggerFactory
import java.io.File
import com.sdc.graphx_example.command_line.RUN_TYPE
import com.sdc.graphx_example.shortestpath.single_source.ShortestPathSingleSourceForward
import com.sdc.graphx_example.App
import com.sdc.graphx_example.shortestpath.ShortestPathProcess
import org.apache.spark.sql.functions._
import com.sdc.graphx_example.test.unit.TestWithSparkSession
import com.sdc.graphx_example.command_line.ParameterDefault

@RunWith(classOf[BlockJUnit4ClassRunner])
class TestShortestPathProcess extends TestWithSparkSession {
    
    private val LOG = LoggerFactory.getLogger(getClass)
    
    @Test
    def testShortestPathSingleSourceForward() = {
        
        val session = getSpark()
        import session.sqlContext.implicits._

        // clean output directory
        val outputDir : String = "target/test/integration/shortest-path/testShortestPathSingleSourceForward/"
        deleteDirectory(outputDir)
        val outputDirFile = new File(outputDir)
        println("Output dir clean? %s".format(!outputDirFile.exists()))

        
        val fileResourceNodes = "/networks/internal/casal-bertone/nodes/";
        val fileUrlNodes = this.getClass().getResource(fileResourceNodes);

        val fileResourceLinks = "/networks/internal/casal-bertone/links/";
        val fileUrlLinks = this.getClass().getResource(fileResourceLinks);
        

        val args = ("--spark-master local --run-type %s --nodes-file %s --links-file %s" + 
        " --sp-source-lon %f --sp-source-lat %f " + 
        " --sp-nearest-distance %d --sp-cost-function %s --output-dir %s --osmc-net-out-format %s")
            .format(RUN_TYPE.SHORTEST_PATH_SINGLE_SOURCE_FORWARD.getValue, fileUrlNodes, fileUrlLinks
                    , 12.53685, 41.89721
                    , 100
                    , ShortestPathSingleSourceForward.COST_FUNCTION.DISTANCE.toString()
                    , outputDir, ParameterDefault.DEFAULT_NETWORK_OUTPUT_FORMAT.getValue)

        App.main(args.split(" "))
        
        //here the spark session has been closed
        getOrCreateSparkSession()

        val expectedVerticesFile = new File(outputDir + App.SHORTEST_PATH_VERTICES_OUTPUT_FILE_NAME)
        assertTrue(expectedVerticesFile.exists())
        assertTrue(expectedVerticesFile.isDirectory())

        val verticesDF = getSpark().read
        .option("header", "true")
        .schema(ShortestPathSingleSourceForward.VERTEX_SHORTEST_PATH_SCHEMA)
        .csv(expectedVerticesFile.getAbsolutePath)
        
        verticesDF.cache
        
        assertTrue(verticesDF.count() > 0)
        
        val vertexVisitedDF = verticesDF.select("*").where($"minCost" < ShortestPathSingleSourceForward.INITIAL_COST && $"predecessor" != -1)
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
        
    @Test
    def testShortestPathStandard() = {
        
        val session = getSpark()
        import session.sqlContext.implicits._

        // clean output directory
        val outputDir : String = "target/test/integration/shortest-path/testShortestPathStandard/"
        deleteDirectory(outputDir)
        val outputDirFile = new File(outputDir)
        println("Output dir clean? %s".format(!outputDirFile.exists()))

        
        val fileResourceNodes = "/networks/internal/casal-bertone/nodes/";
        val fileUrlNodes = this.getClass().getResource(fileResourceNodes);

        val fileResourceLinks = "/networks/internal/casal-bertone/links/";
        val fileUrlLinks = this.getClass().getResource(fileResourceLinks);
        

        val args = ("--spark-master local --run-type %s --nodes-file %s --links-file %s" + 
        " --sp-source-lon %f --sp-source-lat %f --output-dir %s --osmc-net-out-format %s")
            .format(RUN_TYPE.SHORTEST_PATH_STANDARD.getValue, fileUrlNodes, fileUrlLinks
                    , 12.53685, 41.89721, outputDir, ParameterDefault.DEFAULT_NETWORK_OUTPUT_FORMAT.getValue)

        App.main(args.split(" "))
        
        //here the spark session has been closed
        getOrCreateSparkSession()

        val expectedVerticesFile = new File(outputDir + App.SHORTEST_PATH_VERTICES_OUTPUT_FILE_NAME)
        assertTrue(expectedVerticesFile.exists())
        assertTrue(expectedVerticesFile.isDirectory())

        val verticesDF = getSpark().read
        .option("header", "true")
        .schema(ShortestPathProcess.VERTEX_SHORTEST_PATH_STANDARD_SCHEMA)
        .csv(expectedVerticesFile.getAbsolutePath)
        
        verticesDF.cache
        
        assertTrue(verticesDF.count() > 0)
        
        val vertexVisitedDF = verticesDF.select("*")
        .where(col(ShortestPathProcess.LANDMARK_DISTANCE) =!= "null")
        
        vertexVisitedDF.cache
        vertexVisitedDF.show()
        
        val visitedVertexCount = vertexVisitedDF.count()
        assertTrue(visitedVertexCount > 0)
        
        LOG.info("Number of visited nodes: %d".format(visitedVertexCount))
        
        val sourceId = 296057855l
        val source = verticesDF.select("*").where($"id" === sourceId).first()
        
        assertThat(source.getString(1), is(equalTo("%d -> 0".format(sourceId))))
 
    }
 
    @Test
    def testShortestPathCustomCostFunction() = {
        
        val session = getSpark()
        import session.sqlContext.implicits._

        // clean output directory
        val outputDir : String = "target/test/integration/shortest-path/testShortestPathCustomCostFunction/"
        deleteDirectory(outputDir)
        val outputDirFile = new File(outputDir)
        println("Output dir clean? %s".format(!outputDirFile.exists()))

        
        val fileResourceNodes = "/networks/internal/casal-bertone/nodes/";
        val fileUrlNodes = this.getClass().getResource(fileResourceNodes);

        val fileResourceLinks = "/networks/internal/casal-bertone/links/";
        val fileUrlLinks = this.getClass().getResource(fileResourceLinks);
        

        val args = ("--spark-master local --run-type %s --nodes-file %s --links-file %s" + 
        " --sp-source-lon %f --sp-source-lat %f --sp-cost-function %s" + 
        " --sp-graph-repartition %d --output-dir %s --osmc-net-out-format %s")
            .format(RUN_TYPE.SHORTEST_PATH_CUSTOM_COST_FUCNTION.getValue, fileUrlNodes, fileUrlLinks
                    , 12.53685, 41.89721
                    , ShortestPathSingleSourceForward.COST_FUNCTION.DISTANCE.toString()
                    , -1, outputDir, ParameterDefault.DEFAULT_NETWORK_OUTPUT_FORMAT.getValue)

        App.main(args.split(" "))
        
        //here the spark session has been closed
        getOrCreateSparkSession()

        val expectedVerticesFile = new File(outputDir + App.SHORTEST_PATH_VERTICES_OUTPUT_FILE_NAME)
        assertTrue(expectedVerticesFile.exists())
        assertTrue(expectedVerticesFile.isDirectory())

        val verticesDF = getSpark().read
        .option("header", "true")
        .schema(ShortestPathProcess.VERTEX_SHORTEST_PATH_STANDARD_SCHEMA)
        .csv(expectedVerticesFile.getAbsolutePath)
        
        verticesDF.cache
        
        assertTrue(verticesDF.count() > 0)
        
        val vertexVisitedDF = verticesDF.select("*")
        .where(col(ShortestPathProcess.LANDMARK_DISTANCE) =!= "null")
        
        vertexVisitedDF.cache
        vertexVisitedDF.show()
        
        val visitedVertexCount = vertexVisitedDF.count()
        assertTrue(visitedVertexCount > 0)
        
        LOG.info("Number of visited nodes: %d".format(visitedVertexCount))
        
        val sourceId = 296057855l
        val source = verticesDF.select("*").where($"id" === sourceId).first()
        
        assertThat(source.getString(1), is(equalTo("%d -> 0.0".format(sourceId))))
 
    }
    
    
    @Test
    def testShortestPathRandomGraph() = {
        
        val session = getSpark()
        import session.sqlContext.implicits._

        // clean output directory
        val outputDir : String = "target/test/integration/shortest-path/testShortestPathRandomGraph/"
        deleteDirectory(outputDir)
        val outputDirFile = new File(outputDir)
        println("Output dir clean? %s".format(!outputDirFile.exists()))

        
        val fileResourceNodes = "/networks/internal/casal-bertone/nodes/";
        val fileUrlNodes = this.getClass().getResource(fileResourceNodes);

        val fileResourceLinks = "/networks/internal/casal-bertone/links/";
        val fileUrlLinks = this.getClass().getResource(fileResourceLinks);
        

        val args = ("--spark-master local --run-type %s --sp-random-graph-num-vertices %d --output-dir %s --osmc-net-out-format %s")
            .format(RUN_TYPE.SHORTEST_PATH_RANDOM_GRAPH.getValue, 100, outputDir, ParameterDefault.DEFAULT_NETWORK_OUTPUT_FORMAT.getValue)

        App.main(args.split(" "))
        
        //here the spark session has been closed
        getOrCreateSparkSession()

        val expectedVerticesFile = new File(outputDir + App.SHORTEST_PATH_VERTICES_OUTPUT_FILE_NAME)
        assertTrue(expectedVerticesFile.exists())
        assertTrue(expectedVerticesFile.isDirectory())

        val verticesDF = getSpark().read
        .option("header", "true")
        .schema(ShortestPathProcess.VERTEX_SHORTEST_PATH_STANDARD_SCHEMA)
        .csv(expectedVerticesFile.getAbsolutePath)
        
        verticesDF.cache
        
        assertTrue(verticesDF.count() > 0)
        
        val vertexVisitedDF = verticesDF.select("*")
        .where(col(ShortestPathProcess.LANDMARK_DISTANCE) =!= "null")
        
        vertexVisitedDF.cache
        vertexVisitedDF.show()
        
        val visitedVertexCount = vertexVisitedDF.count()
        assertTrue(visitedVertexCount > 0)
        
        LOG.info("Number of visited nodes: %d".format(visitedVertexCount))
 
    }    
    

}
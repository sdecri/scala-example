package com.sdc.scala_example.test.integration

import org.junit.Test
import com.sdc.scala_example.test.unit.TestWithSparkSession
import com.sdc.scala_example.command_line.RUN_TYPE
import com.sdc.scala_example.shortestpath.ShortestPathsCustom
import java.io.File
import com.sdc.scala_example.App

import org.hamcrest.Matchers._
import org.junit.Assert._

@Test
class TestShortestPathProcess extends TestWithSparkSession {
    
    @Test
    def testProcess() = {
        
        // clean output directory
        val outputDir : String = "target/test/integration/shortest_path_process/testProcess/"
        deleteDirectory(outputDir)
        val outputDirFile = new File(outputDir)
        println("Output dir clean? %s".format(!outputDirFile.exists()))

        
        val fileResourceNodes = "/networks/internal/casal-bertone/nodes/";
        val fileUrlNodes = getClass().getResource(fileResourceNodes);

        val fileResourceLinks = "/networks/internal/casal-bertone/links/";
        val fileUrlLinks = getClass().getResource(fileResourceLinks);
        

        val args = "--spark-master local --run-type %s --nodes-file %s --links-file %s --sp-cost-function %s --output-dir %s"
            .format(RUN_TYPE.SHORTEST_PATH.getValue, fileUrlNodes.getFile, fileUrlLinks.getFile
                    , ShortestPathsCustom.COST_FUNCTION.DISTANCE.toString()
                    , outputDir)

        App.main(args.split(" "))


    }
    
}
package com.sdc.graphx_example.test.integration

import org.junit.Test
import com.sdc.graphx_example.test.unit.TestWithSparkSession
import com.sdc.graphx_example.App
import java.io.File

import org.hamcrest.Matchers._
import org.junit.Assert._
import com.sdc.graphx_example.command_line.RUN_TYPE
import org.apache.spark.sql.SQLContext
import scala.collection.Map
import org.junit.runner.RunWith
import org.junit.runners.BlockJUnit4ClassRunner

@RunWith(classOf[BlockJUnit4ClassRunner])
class TestOsmConverterProcess extends TestWithSparkSession {

    @Test
    def testProcess() = {

        val session = getSpark()
        import session.sqlContext.implicits._
        
        
        
        // clean output directory
        val outputDir : String = "target/test/integration/osm_converter/testProcess/"
        deleteDirectory(outputDir)
        val outputDirFile = new File(outputDir)
        println("Output dir clean? %s".format(!outputDirFile.exists()))

        val fileResourceNodes = "/networks/osm/casal-bertone/casal-bertone-node.parquet";
        val fileUrlNodes = this.getClass().getResource(fileResourceNodes);

        val fileResourceWays = "/networks/osm/casal-bertone/casal-bertone-way.parquet";
        val fileUrlWays = this.getClass().getResource(fileResourceWays);

        val linksRepartition = 3

        val args = ("--spark-master local --run-type %s --osm-nodes-file %s --osm-ways-file %s" + 
            " --osmc-links-repartition-output %d --output-dir %s" +
            " --osmc-persist-links true")
            .format(RUN_TYPE.OSM_CONVERTER.getValue, fileUrlNodes, fileUrlWays, linksRepartition, outputDir)

        App.main(args.split(" "))
        
        //here the spark session has been closed
        getOrCreateSparkSession()

        val expectedNodesFile = new File(outputDir + "nodes")
        assertTrue(expectedNodesFile.exists())
        assertTrue(expectedNodesFile.isDirectory())

        val nodesDF = getSpark().read.parquet(expectedNodesFile.getAbsolutePath).orderBy("id")
        nodesDF.cache()
        nodesDF.show()
        assertTrue(nodesDF.count() > 0)

        val expectedLinksFile = new File(outputDir + "links")
        assertTrue(expectedLinksFile.exists())
        assertTrue(expectedLinksFile.isDirectory())

        val linkParquetFiles = expectedLinksFile.listFiles()
        val outputFiles = linkParquetFiles.filter(f => f.getPath().endsWith(".parquet"));
        assertEquals(linksRepartition, outputFiles.length);

        val linksDF = getSpark().read.parquet(expectedLinksFile.getAbsolutePath)
        linksDF.cache
        linksDF.show()
        assertTrue(linksDF.count() > 0)
        
        var actual = linksDF.select("*").where($"tail" === 957255602 && $"head" === 957254675).count()
        assertThat(actual, is(equalTo(1l)))
        
        actual = linksDF.select("*").where($"tail" === 957254675 && $"head" === 957255602).count()
        assertThat(actual, is(equalTo(0l)))
        
        actual = linksDF.select("*").where($"tail" === 295781343 && $"head" === 296057855).count()
        assertThat(actual, is(equalTo(1l)))
        
        actual = linksDF.select("*").where($"tail" === 296057855 && $"head" === 295781343).count()
        assertThat(actual, is(equalTo(1l)))
        
        actual = linksDF.select("*").where($"tail" === 295778585 && $"head" === 295780015).count()
        assertThat(actual, is(equalTo(1l)))
        
        actual = linksDF.select("*").where($"tail" === 295780015 && $"head" === 295778585).count()
        assertThat(actual, is(equalTo(0l)))        
        
    }



}
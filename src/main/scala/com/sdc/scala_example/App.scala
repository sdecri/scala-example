package com.sdc.scala_example

import com.sdc.scala_example.network.Node
import com.sdc.scala_example.network.Link
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import com.sdc.scala_example.network.Link
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.slf4j.LoggerFactory
import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.Edge 
import com.sdc.scala_example.shortestpath.ShortestPathsCustom

/**
 * @author ${user.name}
 */
object App {

    val LOG = LoggerFactory.getLogger(classOf[App])

    def main(args : Array[String]) {


        var conf : SparkConf = new SparkConf()
            .setMaster("local[%s]".format(4))
            .setAppName("scala-example");

        var session : SparkSession = null
        try {

            session = SparkSession.builder().config(conf).getOrCreate()
            

        } catch {
            case t : Throwable => t.printStackTrace()
        } finally {
            if (session != null)
                session.close()
        }
    }


        
//    private def runGraphFrameExample() = {
        //            val nodesRowRDD : RDD[Row] = session.sparkContext.makeRDD(nodes.map(_.toRow()))
//            val nodesDF : Dataset[Row] = session.createDataFrame(nodesRowRDD, Node.SCHEMA)
//            LOG.info("Nodes count = %s".format(nodesDF.count()))
//            nodesDF.printSchema()
//            nodesDF.show()
//
//            val linksRowRDD : RDD[Row] = session.sparkContext.makeRDD(links.map(_.toRow()))
//            val linksDF : Dataset[Row] = session.createDataFrame(linksRowRDD, Link.SCHEMA)
//            LOG.info("Links count = %s".format(linksDF.count()))
//            linksDF.printSchema()
//            linksDF.show()
            
            // GRAPH_FRAME
//            val gf : GraphFrame = GraphFrame(nodesDF, linksDF)
//            gf.vertices.show()
        
        //            val bfsResult = graph.bfs.fromExpr("id = 1").toExpr("id = 6").run()
//            bfsResult.show()
            
//            val dst = "6"
//            val gfSpResult = gf.shortestPaths.landmarks(Seq(dst)).run()
//            gfSpResult.printSchema()
//            gfSpResult.show()

//    }
}

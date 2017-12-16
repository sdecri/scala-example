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

/**
 * @author ${user.name}
 */
object App {

    val LOG = LoggerFactory.getLogger(classOf[App])

    def main(args : Array[String]) {

        var nodes = createNodes()

        var links = createLinks(nodes)

        var conf : SparkConf = new SparkConf()
            .setMaster("local[%s]".format(4))
            .setAppName("scala-example");

        var session : SparkSession = null
        try {

            session = SparkSession.builder().config(conf).getOrCreate()
            
            val nodesRdd = session.sparkContext.parallelize(nodes)
            val vertices = nodesRdd.map(n => (n.getId, n))
            
            val linksRdd = session.sparkContext.parallelize(links)
            val edges = linksRdd.map(l => Edge(l.getTail().getId(), l.getHead().getId(), l))
            
            val dest = 6l
            
            val graphx = Graph(vertices, edges)
            val sp = ShortestPaths.run(graphx, Seq(dest))
            LOG.info("Shortest path to %s result:".format(dest))
            LOG.info("> Vertices:")
            println(sp.vertices.collect().mkString(System.lineSeparator()))
            LOG.info("> Edges:")
            println(sp.edges.collect().mkString(System.lineSeparator()))
            
            

        } catch {
            case t : Throwable => t.printStackTrace()
        } finally {
            if (session != null)
                session.close()
        }
    }

    def createLinks(nodes : List[com.sdc.scala_example.network.Node]) = List(
        new Link(1, Node.findById(1, nodes).get, Node.findById(2, nodes).get, 40, 10)
        , new Link(2, Node.findById(1, nodes).get, Node.findById(3, nodes).get, 20, 10)
        , new Link(3, Node.findById(3, nodes).get, Node.findById(2, nodes).get, 10, 10)
        , new Link(4, Node.findById(2, nodes).get, Node.findById(5, nodes).get, 50, 10)
        , new Link(5, Node.findById(3, nodes).get, Node.findById(4, nodes).get, 30, 10)
        , new Link(6, Node.findById(3, nodes).get, Node.findById(6, nodes).get, 60, 10)
        , new Link(7, Node.findById(2, nodes).get, Node.findById(4, nodes).get, 10, 10)
        , new Link(8, Node.findById(5, nodes).get, Node.findById(4, nodes).get, 20, 10)
        , new Link(9, Node.findById(4, nodes).get, Node.findById(5, nodes).get, 40, 10)
        , new Link(10, Node.findById(4, nodes).get, Node.findById(6, nodes).get, 20, 10)
        , new Link(11, Node.findById(5, nodes).get, Node.findById(6, nodes).get, 50, 10)
        , new Link(12, Node.findById(4, nodes).get, Node.findById(6, nodes).get, 30, 10)
    )

    def createNodes() : List[Node] = List(new Node(1, 1.0, 1.0), new Node(2, 3.0, 0.0),
        new Node(3, 3.0, 3.0), new Node(4, 5.0, 0.0), new Node(5, 5.0, 3.0),
        new Node(6, 7.0, 1.0))
        
        
    private def runGraphFrameExample() = {
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

    }
}

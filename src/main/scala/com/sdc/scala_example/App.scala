package com.sdc.scala_example

import com.sdc.scala_example.network.Node
import com.sdc.scala_example.network.Link
import org.graphframes.GraphFrame
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

/**
 * @author ${user.name}
 */
object App {

    val LOG = LoggerFactory.getLogger(classOf[App]) 
    
    def main(args: Array[String]) {
        
        LOG.info("ciao")
        
        var nodes: List[Node] = List(new Node(1, 1.0, 1.0), new Node(2, 3.0, 0.0), new Node(3, 3.0, 3.0)
        , new Node(4, 5.0, 0.0), new Node(5, 5.0, 3.0), new Node(6, 7.0, 1.0))
        
        var links :List[Link] = List(new Link(1, Node.findById(1, nodes).get, Node.findById(2, nodes).get, 40, 10)
        ,new Link(2, Node.findById(1, nodes).get, Node.findById(3, nodes).get, 20, 10)
        ,new Link(3, Node.findById(3, nodes).get, Node.findById(2, nodes).get, 10, 10)
        ,new Link(4, Node.findById(2, nodes).get, Node.findById(5, nodes).get, 50, 10)
        ,new Link(5, Node.findById(3, nodes).get, Node.findById(4, nodes).get, 30, 10)
        ,new Link(6, Node.findById(3, nodes).get, Node.findById(6, nodes).get, 60, 10)
        ,new Link(7, Node.findById(2, nodes).get, Node.findById(4, nodes).get, 10, 10)
        ,new Link(8, Node.findById(5, nodes).get, Node.findById(4, nodes).get, 20, 10)
        ,new Link(9, Node.findById(4, nodes).get, Node.findById(5, nodes).get, 40, 10)
        ,new Link(10, Node.findById(4, nodes).get, Node.findById(6, nodes).get, 20, 10)
        ,new Link(11, Node.findById(5, nodes).get, Node.findById(6, nodes).get, 50, 10)
        )

        var conf: SparkConf = new SparkConf()
            .setMaster("local[%s]".format(4))
            .setAppName("scala-example");

        var session: SparkSession = null
        try {

            session = SparkSession.builder().config(conf).getOrCreate()

            val nodesRowRDD: RDD[Row] = session.sparkContext.makeRDD(nodes.map(_.toRow()))
            val nodesDF: Dataset[Row] = session.createDataFrame(nodesRowRDD, Node.SCHEMA)
            println("Nodes count = %s".format(nodesDF.count()))
            nodesDF.show()

            val linksRowRDD: RDD[Row] = session.sparkContext.makeRDD(links.map(_.toRow()))
            val linksDF: Dataset[Row] = session.createDataFrame(linksRowRDD, Link.SCHEMA)
            println("Links count = %s".format(linksDF.count()))
            linksDF.show()

            val graph: GraphFrame = GraphFrame(nodesDF, linksDF)
            graph.vertices.show()

            val bfsResult = graph.bfs.fromExpr("id = 1").toExpr("id = 6").run()
            bfsResult.show()

        } catch {
            case t: Throwable => t.printStackTrace()
        } finally {
            if (session != null)
                session.close()
        }

    }

}

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

/**
 * @author ${user.name}
 */
object App {

    def main(args: Array[String]) {

        var n1 = new Node(1, 0.0, 0.0)
        var n2 = new Node(2, 1.0, 1.0)
        var n3 = new Node(3, 2.0, 0.0)

        var nodes: List[Node] = List(n1, n2, n3)

        var link1 = new Link(1, n1, n2, 100)
        var link2 = new Link(2, n2, n3, 100)
        var link3 = new Link(2, n1, n3, 250)

        var links: List[Link] = List(link1, link2, link3)

        var conf: SparkConf = new SparkConf()
            .setMaster("local[%s]".format(4))
            .setAppName("scala-example");

        var session: SparkSession = null
        try {

            session = SparkSession.builder().config(conf).getOrCreate()

            val nodesRowRDD :RDD[Row] = session.sparkContext.makeRDD(nodes.map(_.toRow()))
            val nodesDF : Dataset[Row] = session.createDataFrame(nodesRowRDD, Node.SCHEMA)
            println("Nodes count = %s".format(nodesDF.count()))
            nodesDF.show()

            val linksRowRDD :RDD[Row] = session.sparkContext.makeRDD(links.map(_.toRow()))
            val linksDF : Dataset[Row] = session.createDataFrame(linksRowRDD, Link.SCHEMA)
            println("Links count = %s".format(linksDF.count()))
            linksDF.show()
            
            val graph : GraphFrame = GraphFrame(nodesDF, linksDF)
            graph.vertices.show()
            
            val bfsResult = graph.bfs.fromExpr("id = 1").toExpr("id = 3").run()
            bfsResult.show()

        } catch {
            case t: Throwable => t.printStackTrace()
        } finally {
            if (session != null)
                session.close()
        }

    }

}

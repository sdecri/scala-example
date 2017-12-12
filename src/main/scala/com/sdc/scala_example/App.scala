package com.sdc.scala_example

import com.sdc.scala_example.network.Node
import com.sdc.scala_example.network.Link
import org.graphframes.GraphFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

/**
 * @author ${user.name}
 */
object App {

    def main(args: Array[String]) {

        var n1 = new Node(1, 0.0, 0.0)
        var n2 = new Node(2, 1.0, 1.0)
        var n3 = new Node(3, 2.0, 0.0)

        var link1 = new Link(1, n1, n2, 100.0)
        var link2 = new Link(2, n2, n3, 100.0)
        var link3 = new Link(2, n2, n3, 250.0)

        var conf: SparkConf = new SparkConf()
            .setMaster("local[%s]".format(4))
            .setAppName("scala-example");

        var session: SparkSession = null
        try {

            session = SparkSession.builder().config(conf).getOrCreate()
            
            
            
            

        } catch {
            case t: Throwable => t.printStackTrace()
        }finally {
            if(session != null)
                session.close()
        }

    }

}

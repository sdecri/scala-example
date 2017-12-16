package com.sdc.scala_example.test.unit

import org.junit.Test
import com.sdc.scala_example.network.Link
import com.sdc.scala_example.network.Node
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.slf4j.LoggerFactory
import com.sdc.scala_example.shortestpath.ShortestPathsCustom
import org.hamcrest.Matchers._
import org.junit.Assert._
import com.sdc.scala_example.shortestpath.VertexShortestPath
import scala.collection.Map
import com.sdc.scala_example.shortestpath.VertexShortestPath


@Test
class TestShortestPathCustom {
    
    val LOG = LoggerFactory.getLogger(classOf[App])

    
    def createLinks(nodes : List[com.sdc.scala_example.network.Node]) = List(
        new Link(1, Node.findById(1, nodes).get, Node.findById(3, nodes).get, 20, 10)
        , new Link(2, Node.findById(1, nodes).get, Node.findById(2, nodes).get, 40, 10)
        , new Link(3, Node.findById(3, nodes).get, Node.findById(2, nodes).get, 10, 10)
        , new Link(4, Node.findById(2, nodes).get, Node.findById(5, nodes).get, 50, 10)
        , new Link(5, Node.findById(3, nodes).get, Node.findById(4, nodes).get, 30, 10)
        , new Link(6, Node.findById(3, nodes).get, Node.findById(6, nodes).get, 60, 10)
        , new Link(7, Node.findById(2, nodes).get, Node.findById(4, nodes).get, 10, 10)
        , new Link(8, Node.findById(5, nodes).get, Node.findById(4, nodes).get, 40, 10)
        , new Link(9, Node.findById(4, nodes).get, Node.findById(5, nodes).get, 20, 10)
        , new Link(10, Node.findById(4, nodes).get, Node.findById(6, nodes).get, 20, 10)
        , new Link(11, Node.findById(5, nodes).get, Node.findById(6, nodes).get, 50, 10)
        , new Link(12, Node.findById(4, nodes).get, Node.findById(6, nodes).get, 30, 10)
    )

    def createNodes() : List[Node] = List(new Node(1, 1.0, 1.0), new Node(2, 3.0, 0.0),
        new Node(3, 3.0, 3.0), new Node(4, 5.0, 0.0), new Node(5, 5.0, 3.0),
        new Node(6, 7.0, 1.0))
        
        
    private def testSP(spType :ShortestPathsCustom.COST_FUNCTION.Value, expected :Map[Long, VertexShortestPath]) {
        
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
            
            val graphx = Graph(vertices, edges)

            
            LOG.info("####################################################")
            val source = 1l
            
            var spDistance = ShortestPathsCustom.run(graphx, source, spType)
            LOG.info("Shortest path custom (with arc cost function = %s)".format(spType))
            LOG.info("> Shortest path from %s result:".format(source))
            LOG.info(">> Vertices:")
            val spVertices = spDistance.vertices.collectAsMap()
            println(spVertices.mkString(System.lineSeparator()))
//            LOG.info(">> Edges:")
//            println(spDistance.edges.collect().mkString(System.lineSeparator()))
            
            for((k, v) <- expected){
                assertThat(spVertices.get(k).get.getMinCost(), is(equalTo(v.getMinCost())))
                assertThat(spVertices.get(k).get.getPredecessorLink(), is(equalTo(v.getPredecessorLink())))
            }
            
            
        } catch {
            case e : Exception => e.printStackTrace()
        } finally {
            if (session != null)
                session.close()
        }
        
    }    
        
    @Test
    def testSPDistance(){
        testSP(ShortestPathsCustom.COST_FUNCTION.DISTANCE,
            Map(1l -> new VertexShortestPath(0.0,-1)
            , 2l -> new VertexShortestPath(30.0,3l)
            , 3l -> new VertexShortestPath(20.0,1l)
            , 4l -> new VertexShortestPath(40.0,7l)
            , 5l -> new VertexShortestPath(60.0,9l)
            , 6l -> new VertexShortestPath(60.0,10l)
            )
        )

    }
    
    @Test
    def testSPTravelTime(){
        
        testSP(ShortestPathsCustom.COST_FUNCTION.TRAVEL_TIME,
            Map(1l -> new VertexShortestPath(0.0,-1)
            , 2l -> new VertexShortestPath(3.0,3l)
            , 3l -> new VertexShortestPath(2.0,1l)
            , 4l -> new VertexShortestPath(4.0,7l)
            , 5l -> new VertexShortestPath(6.0,9l)
            , 6l -> new VertexShortestPath(6.0,10l)
            )
        )

    }
    
    
}
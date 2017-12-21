package com.sdc.scala_example.test.unit

import org.junit.Test
import com.sdc.scala_example.network.Link
import com.sdc.scala_example.network.Node
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.slf4j.LoggerFactory
import com.sdc.scala_example.shortestpath.single_source.ShortestPathSingleSourceForward
import org.hamcrest.Matchers._
import org.junit.Assert._
import scala.collection.Map
import com.sdc.scala_example.shortestpath.single_source.VertexShortestPath


@Test
class TestShortestPathSingleSourceForward extends TestWithSparkSession {
    
    val LOG = LoggerFactory.getLogger(classOf[TestShortestPathSingleSourceForward])

    
    def createLinks(nodes : List[com.sdc.scala_example.network.Node]) = List(
        new Link(1, 1, 3, 20, 10)
        , new Link(2, 1, 2, 40, 10)
        , new Link(3, 3, 2, 10, 10)
        , new Link(4, 2, 5, 50, 10)
        , new Link(5, 3, 4, 30, 10)
        , new Link(6, 3, 6, 60, 10)
        , new Link(7, 2, 4, 10, 10)
        , new Link(8, 5, 4, 40, 10)
        , new Link(9, 4, 5, 20, 10)
        , new Link(10, 4, 6, 20, 10)
        , new Link(11, 5, 6, 50, 10)
        , new Link(12, 4, 6, 30, 10)
    )

    def createNodes() : List[Node] = List(new Node(1, 1.0, 1.0), new Node(2, 3.0, 0.0),
        new Node(3, 3.0, 3.0), new Node(4, 5.0, 0.0), new Node(5, 5.0, 3.0),
        new Node(6, 7.0, 1.0))
        
        
    private def testSP(spType :ShortestPathSingleSourceForward.COST_FUNCTION.Value, expected :Map[Long, VertexShortestPath]) {
        
        var nodes = createNodes()
        var links = createLinks(nodes)

        val nodesRdd = getSpark().sparkContext.parallelize(nodes)
        val vertices = nodesRdd.map(n => (n.getId, n))
        
        val linksRdd = getSpark().sparkContext.parallelize(links)
        val edges = linksRdd.map(l => Edge(l.getTail(), l.getHead(), l))
        
        val graphx = Graph(vertices, edges)

        
        LOG.info("####################################################")
        val source = 1l
        
        var spDistance = ShortestPathSingleSourceForward.run(graphx, source, spType)
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
            

        
    }    
        
    @Test
    def testSPDistance(){
        testSP(ShortestPathSingleSourceForward.COST_FUNCTION.DISTANCE,
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
        
        testSP(ShortestPathSingleSourceForward.COST_FUNCTION.TRAVEL_TIME,
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
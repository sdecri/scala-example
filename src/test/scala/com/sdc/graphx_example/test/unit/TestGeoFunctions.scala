package com.sdc.graphx_example.test.unit

import org.junit.Test
import com.sdc.graphx_example.network._
import org.hamcrest.Matchers._
import org.junit.Assert._
import org.junit.runner.RunWith
import org.junit.runners.BlockJUnit4ClassRunner
import com.sdc.graphx_example.geometry.GeometryUtils
import com.vividsolutions.jts.geom.Coordinate
import org.apache.spark.sql.Encoders


@RunWith(classOf[BlockJUnit4ClassRunner])
class TestGeoFunctions extends TestWithSparkSession{
    
    @Test
    def testGetNearestNode() = {
        
        val list = List(Node(1l, SimplePoint(12.5364106f, 41.8960846f))
                        , Node(2l, SimplePoint(12.5361810f, 41.8966241f)))
        
        val nodesDS = getSpark().createDataset(list)(Encoders.product[Node])
        
        nodesDS.show()
        
        var sourcePoint = GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(12.53647, 41.89617))
        var nearestNodeOption = GeoFunctions.getNearestNode(sourcePoint, nodesDS, getSpark())
        assertThat(nearestNodeOption.isDefined, is(true))
        var nearestNode = nearestNodeOption.get
        assertThat(nearestNode.getId, is(1l))
        
        
        sourcePoint = GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(12.53627, 41.89663))
        nearestNodeOption = GeoFunctions.getNearestNode(sourcePoint, nodesDS, getSpark())
        assertThat(nearestNodeOption.isDefined, is(true))
        nearestNode = nearestNodeOption.get
        assertThat(nearestNode.getId, is(2l))
        
        
        sourcePoint = GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(12.53957, 41.89361))
        nearestNodeOption = GeoFunctions.getNearestNode(sourcePoint, nodesDS, getSpark(), 100, 1)
        assertThat(nearestNodeOption.isDefined, is(false))
        
        sourcePoint = GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(12.53957, 41.89361))
        nearestNodeOption = GeoFunctions.getNearestNode(sourcePoint, nodesDS, getSpark(), 100, 2)
        assertThat(nearestNodeOption.isDefined, is(true))
        nearestNode = nearestNodeOption.get
        assertThat(nearestNode.getId, is(1l))    
        
        
        sourcePoint = GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(12.53957, 41.89361))
        nearestNodeOption = GeoFunctions.getNearestNode(sourcePoint, nodesDS, getSpark(), 100, 2, 3)
        assertThat(nearestNodeOption.isDefined, is(false))
        
        
        sourcePoint = GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(12.53957, 41.89361))
        nearestNodeOption = GeoFunctions.getNearestNode(sourcePoint, nodesDS, getSpark(), 100, 3, 3)
        assertThat(nearestNodeOption.isDefined, is(true))
        nearestNode = nearestNodeOption.get
        assertThat(nearestNode.getId, is(1l))    
    
    }
    
    @Test
    def testGetNearestNodeOnRealNetwork() = {
        
        val fileResourceNodes = "/networks/internal/casal-bertone/nodes/";
        val fileUrlNodes = this.getClass().getResource(fileResourceNodes);
        
        val nodesDS = getSpark().read.schema(Encoders.product[Node].schema).json(fileUrlNodes.getFile).as(Encoders.product[Node])
        
        nodesDS.show()
        
        var sourcePoint = GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(12.53685, 41.89721))
        var nearestNodeOption = GeoFunctions.getNearestNode(sourcePoint, nodesDS, getSpark(), 100, 3, 3)
        assertThat(nearestNodeOption.isDefined, is(true))
        var nearestNode = nearestNodeOption.get
        assertThat(nearestNode.getId, is(296057855l))
        
        
        
    }
    
    
    
}
package com.sdc.graphx_example.test.unit

import org.junit.Test
import com.sdc.graphx_example.network.Node
import com.sdc.graphx_example.network.GeoFunctions
import com.vividsolutions.jts.geom.Point
import com.sdc.graphx_example.geometry.GeometryUtils
import com.vividsolutions.jts.geom.Coordinate
import org.hamcrest.Matchers._
import org.junit.Assert._

import org.junit.runner.RunWith
import org.junit.runners.BlockJUnit4ClassRunner

@RunWith(classOf[BlockJUnit4ClassRunner])
class TestGeoFunctions extends TestWithSparkSession {
    
    @Test
    def testGetNearestNode() = {
        
        val nodesDF = getSpark()
        .createDataFrame(getSpark().sparkContext.makeRDD(List(new Node(1l, 12.5364106, 41.8960846), new Node(2l, 12.5361810, 41.8966241)).map(_.toRow)), Node.SCHEMA)
        
        nodesDF.show()
        
        var sourcePoint = GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(12.53647, 41.89617))
        var nearestNodeOption = GeoFunctions.getNearestNode(sourcePoint, nodesDF, getSpark())
        assertThat(nearestNodeOption.isDefined, is(true))
        var nearestNode = nearestNodeOption.get
        assertThat(nearestNode.getId, is(1l))
        
        
        sourcePoint = GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(12.53627, 41.89663))
        nearestNodeOption = GeoFunctions.getNearestNode(sourcePoint, nodesDF, getSpark())
        assertThat(nearestNodeOption.isDefined, is(true))
        nearestNode = nearestNodeOption.get
        assertThat(nearestNode.getId, is(2l))
        
        
        sourcePoint = GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(12.53957, 41.89361))
        nearestNodeOption = GeoFunctions.getNearestNode(sourcePoint, nodesDF, getSpark(), 100, 1)
        assertThat(nearestNodeOption.isDefined, is(false))
        
        sourcePoint = GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(12.53957, 41.89361))
        nearestNodeOption = GeoFunctions.getNearestNode(sourcePoint, nodesDF, getSpark(), 100, 2)
        assertThat(nearestNodeOption.isDefined, is(true))
        nearestNode = nearestNodeOption.get
        assertThat(nearestNode.getId, is(1l))    
        
        
        sourcePoint = GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(12.53957, 41.89361))
        nearestNodeOption = GeoFunctions.getNearestNode(sourcePoint, nodesDF, getSpark(), 100, 2, 3)
        assertThat(nearestNodeOption.isDefined, is(false))
        
        
        sourcePoint = GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(12.53957, 41.89361))
        nearestNodeOption = GeoFunctions.getNearestNode(sourcePoint, nodesDF, getSpark(), 100, 3, 3)
        assertThat(nearestNodeOption.isDefined, is(true))
        nearestNode = nearestNodeOption.get
        assertThat(nearestNode.getId, is(1l))    
    
    }
    
    
}
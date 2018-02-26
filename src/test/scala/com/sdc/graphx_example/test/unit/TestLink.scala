package com.sdc.graphx_example.test.unit

import org.junit.Test
import com.sdc.graphx_example.network._
import org.hamcrest.Matchers._
import org.junit.Assert._
import org.junit.runner.RunWith
import org.junit.runners.BlockJUnit4ClassRunner


@RunWith(classOf[BlockJUnit4ClassRunner])
class TestLink {
    
    @Test
    def testLinkCreation() = {
        
        var n1 = Node(1, SimplePoint(1f, 1f))
        var n2 = Node(2, SimplePoint(2f, 2f))
        var n3 = Node(3, SimplePoint(3f, 3f))
        
        var link1 : Link = Link(1, n1.getId(), n2.getId(), 100, 50, DrivingDirection.START2END.getValue(), Array.empty)
        
        var link2 : Link = Link(1, n1.getId(), n2.getId(), 100, 50, DrivingDirection.START2END.getValue(), Array.empty)
        assertThat(link1, is(equalTo(link2)))
        
        var link3 : Link = Link(1, n1.getId(), n3.getId(), 100, 50, DrivingDirection.START2END.getValue(), Array.empty)
        assertThat(link1, is(not(equalTo(link3))))
        
        var link4 : Link = Link(2, n1.getId(), n3.getId(), 100, 50, DrivingDirection.START2END.getValue(), Array.empty)
        assertThat(link1, is(not(equalTo(link4))))
        
        var link5 : Link = Link(1, n1.getId(), n3.getId(), 50, 50, DrivingDirection.START2END.getValue(), Array.empty)
        assertThat(link1, is(not(equalTo(link5))))
        
        var link6 : Link = null
        assertThat(link1, is(not(equalTo(link6))))
        
        var link7 : Link = link1
        assertThat(link1, is(equalTo(link7)))
        
        var link8 = Link(8, n1.getId(), n2.getId(), 1000, 10, DrivingDirection.START2END.getValue(), Array.empty)
        assertThat(link8.getTravelTime(), is(equalTo(100.0f)))
    }
    
}
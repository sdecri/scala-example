package com.sdc.scala_example.test.unit

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
        
        var n1 = new Node(1, 1, 1)
        var n2 = new Node(2, 2, 2)
        var n3 = new Node(3, 3, 3)
        
        var link1 : Link = Link(1, n1.getId(), n2.getId(), 100, 50, Array.empty)
        
        var link2 : Link = Link(1, n1.getId(), n2.getId(), 100, 50, Array.empty)
        assertThat(link1, is(equalTo(link2)))
        
        var link3 : Link = Link(1, n1.getId(), n3.getId(), 100, 50, Array.empty)
        assertThat(link1, is(not(equalTo(link3))))
        
        var link4 : Link = Link(2, n1.getId(), n3.getId(), 100, 50, Array.empty)
        assertThat(link1, is(not(equalTo(link4))))
        
        var link5 : Link = Link(1, n1.getId(), n3.getId(), 50, 50, Array.empty)
        assertThat(link1, is(not(equalTo(link5))))
        
        var link6 : Link = null
        assertThat(link1, is(not(equalTo(link6))))
        
        var link7 : Link = link1
        assertThat(link1, is(equalTo(link7)))
        
        var link8 = Link(8, n1.getId(), n2.getId(), 1000, 10, Array.empty)
        assertThat(link8.getTravelTime(), is(equalTo(100.0f)))
    }
    
}
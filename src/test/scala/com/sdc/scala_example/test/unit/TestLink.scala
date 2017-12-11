package com.sdc.scala_example.test.unit

import org.junit.Test
import com.sdc.scala_example.network._
import org.hamcrest.Matchers._
import org.junit.Assert._

@Test
class TestLink {
    
    @Test
    def testLinkCreation() = {
        
        var n1 = new Node(1, 1.0, 1.0)
        var n2 = new Node(2, 2.0, 2.0)
        var n3 = new Node(3, 3.0, 3.0)
        
        var link1 : Link = new Link(1, n1, n2)
        link1.setLength(100.0)
        
        var link2 : Link = new Link(1, n1, n2)
        link2.setLength(100.0)
        assertThat(link1, is(equalTo(link2)))
        
        var link3 : Link = new Link(1, n1, n3)
        link3.setLength(100.0)
        assertThat(link1, is(not(equalTo(link3))))
        
        var link4 : Link = new Link(2, n1, n3)
        link4.setLength(100.0)
        assertThat(link1, is(not(equalTo(link4))))
        
        var link5 : Link = new Link(1, n1, n3)
        link5.setLength(50.0)
        assertThat(link1, is(not(equalTo(link5))))
        
        var link6 : Link = null
        assertThat(link1, is(not(equalTo(link6))))
        
        var link7 : Link = link1
        assertThat(link1, is(equalTo(link7)))
    }
    
}
package com.sdc.graphx_example.test.unit

import org.junit.Test
import com.sdc.graphx_example.network._
import org.hamcrest.Matchers._
import org.junit.Assert._
import org.junit.runner.RunWith
import org.junit.runners.BlockJUnit4ClassRunner


@RunWith(classOf[BlockJUnit4ClassRunner])
class TestNode {
    
    @Test
    def testNodeCreation() = {
        
        var n1 = Node(1, SimplePoint(1.0f, 1.0f))
        
        var n2 = Node(2, SimplePoint(1.0f, 1.0f))
        
        assertThat(n1, is(not(equalTo(n2))))
        
    }
    
}
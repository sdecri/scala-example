

package com.sdc.scala_example.test.unit

import org.junit.Test
import com.sdc.scala_example.network.Node
import org.hamcrest.Matchers._
import org.junit.Assert._

@Test
class TestNode {
    
    @Test
    def testNodeCreation() = {
        
        var n1 = new Node
        n1.setId(1)
        n1.setLon(1.0)
        n1.setLat(1.0)
        
        var n2 = new Node(2, 1.0, 1.0)
        
        assertThat(n1, is(not(equalTo(n2))))
        
    }
}
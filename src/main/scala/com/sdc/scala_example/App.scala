package com.sdc.scala_example

import com.sdc.scala_example.network.Node

/**
 * @author ${user.name}
 */
object App {

    def foo(x: Array[String]) = x.foldLeft("")((a, b) => a + b)

    def main(args: Array[String]) {

        var n1 = new Node
        n1.setId(1)
        n1.setLon(1.0)
        n1.setLat(1.0)
        
        var n2 = new Node
        n2.setId(1)
        n2.setLon(1.0)
        n2.setLat(2.0)
        
        println(n1.equals(n2))
    }

}

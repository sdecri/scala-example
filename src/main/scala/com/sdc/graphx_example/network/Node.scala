package com.sdc.graphx_example.network

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.Encoders

case class Node(id :Long, point :SimplePoint) extends Serializable {

    def getId(): Long = this.id
    def getPoint() :SimplePoint = this.point

    def toRow(): Row = Row(id, point.lon, point.lat)
    
    override def hashCode(): Int = id.toInt

    def canEqual(a: Any) = a.isInstanceOf[Node]
    override def equals(that: Any) = {
        that match {
            case other: Node => this.canEqual(other) && other.getId() == id && other.getPoint() == point
            case _ => false
        }
    }
    
    override def toString() : String = "ID = %s, POINT = %s".format(id, point)

}


object Node {
    
    val ENCODER = Encoders.product[Node]
    
    val CSV_OPTIONS = Map("header" -> "true")
    
    val SCHEMA_CSV = StructType(
        List(
                StructField("id", LongType)
                , StructField("lon", FloatType)
                , StructField("lat", FloatType)
        )        
    )
    
    val POINT = "point"
    
    def findById(id :Long, nodes :List[Node]) : Option[Node] = {
        var l = nodes.filter(_.getId() == id)
        return if (l.isEmpty) None else Some(l(0))
    }
    
    def fromRow(row :Row) : Node = 
        new Node(row.getLong(0), SimplePoint(
                row.getFloat(1)
                , row.getFloat(2)))
    
}


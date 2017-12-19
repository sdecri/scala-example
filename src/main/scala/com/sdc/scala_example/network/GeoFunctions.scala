package com.sdc.scala_example.network

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import com.vividsolutions.jts.geom.Point
import com.sdc.scala_example.geometry.GeometryUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.Dataset
import org.slf4j.LoggerFactory
import scala.collection.immutable.List

object GeoFunctions {

    private val LOG = LoggerFactory.getLogger(getClass)
    
    /**
     * Find the nearest node to the given source point among all the elements included in the specified dataframe
     * @param sourcePoint
     * @param nodesDF dataframe of nodes
     * @param session spark session
     * @param firstDistance first considered distance in which to search
     * @param nAttempt number of attempts. If no node is found the search continue considered a larger distance in which to search
     * @param extensionFactor factor used to enlarge the distance in which to search
     */
    def getNearestNode(sourcePoint : Point, nodesDF :Dataset[Row], session :SparkSession
            , firstDistance :Int = 1000, nAttempt :Int = 3, extensionFactor :Int = 10) : Option[Node] = {

        val sqlContext = new SQLContext(session.sparkContext)
        import sqlContext.implicits._
        import org.apache.spark.sql.functions._

        /**
         * A user defined function to compute the distance between the source point and each node in the dataset
         */
        def computeDistance(lon1 : Double, lat1 : Double) = udf((lon2 : Double, lat2 : Double) => GeometryUtils.getDistance(lon1, lat1, lon2, lat2))

        nodesDF.cache

        var nearestNodeRow : Option[Row] = None
        var attempt = 0
        while (nearestNodeRow.isEmpty && attempt < nAttempt) {
            val distanceBBox = firstDistance * ((attempt * extensionFactor) max 1)
            val ur = GeometryUtils.determineCoordinateInDistance(sourcePoint.getX, sourcePoint.getY, 45, distanceBBox)
            val bl = GeometryUtils.determineCoordinateInDistance(sourcePoint.getX, sourcePoint.getY, 45 + 180, distanceBBox)

            val distanceColumn = "distanceToPoint"
            val nearestNodeList = nodesDF.select("*")
                .where($"longitude" <= ur.x && $"longitude" >= bl.x && $"latitude" <= ur.y && $"latitude" >= bl.y)
                .withColumn(distanceColumn, computeDistance(sourcePoint.getX, sourcePoint.getY)($"longitude", $"latitude"))
                .orderBy(asc(distanceColumn))
//                .drop(col(distanceColumn))
                .collect()

                //debug_sdc
            println(nearestNodeList.mkString(System.lineSeparator()))
                
            if (!nearestNodeList.isEmpty)
                nearestNodeRow = Some(nearestNodeList(0))

            attempt += 1
        }

        var nearestNode :Option[Node] = None
        if (nearestNodeRow.isEmpty)
            LOG.warn("No nearest node found to the current source point: %s".format(sourcePoint))   
        else
            nearestNode = Some(Node.fromRow(nearestNodeRow.get)) 

        nearestNode
    }

}
package com.sdc.graphx_example.osm

import java.io.File
import org.apache.spark.sql._
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.functions._
import com.sdc.graphx_example.network.Link
import scala.collection.mutable.WrappedArray
import com.sdc.graphx_example.geometry.GeometryUtils
import org.slf4j.LoggerFactory
import com.sdc.graphx_example.command_line.AppContext

/**
 * Class that import osm parquet produced with osm-parquetizer
 * <a href="http://wiki.openstreetmap.org/wiki/Osm-parquetizer">http://wiki.openstreetmap.org/wiki/Osm-parquetizer</a>
 * creating <code>Node</code> and <code>Link</code> dataframe.
 */
object OsmParquetConverter {
    
    private type nodeType = (Int, Long, Double, Double)
    
    private val LOG = LoggerFactory.getLogger(getClass)
    


    def convertToNetwork[VD](sparkSession : SparkSession, context :AppContext) : Unit = {

        LOG.info("Convert OSM parquet to internal network parquet with context: %s".format(context))
        
        var allNodeDF = convertNodes(sparkSession, context)
        allNodeDF.cache()
        
        LOG.info("Number of all imported nodes: %d".format(allNodeDF.count()))
        
        var net = convertLinks(sparkSession, allNodeDF, context)
        
        val nodesParquetFilePath = context.getOutputDir + "nodes"
        var nodeDF = net._1
        if(context.getOsmConverterPersistNodes){
            if (context.getNodesRepartitionOutput > 0)
                nodeDF = nodeDF.repartition(context.getNodesRepartitionOutput)
            nodeDF.write.mode(SaveMode.Overwrite).parquet(nodesParquetFilePath)
            LOG.info("Number of network nodes: %d".format(nodeDF.count()))
        }
        
        val linksParquetFilePath = context.getOutputDir + "links"
        var linkDS = net._2
        linkDS.cache()
        if(context.getOsmConverterPersistLinks) {
            if (context.getLinksRepartitionOutput > 0)
                linkDS = linkDS.repartition(context.getLinksRepartitionOutput)
            linkDS.write.mode(SaveMode.Overwrite).parquet(linksParquetFilePath)
            LOG.info("Number of network links: %d".format(linkDS.count()))
        }

    }

    private def convertNodes(sparkSession : SparkSession, context :AppContext) : DataFrame = {
        val nodesOsmDF = sparkSession.read.parquet(context.getOsmNodesFilePath)
        nodesOsmDF.select("id", "latitude", "longitude")
    }

    private def convertLinks(sparkSession : SparkSession, nodeDF : DataFrame, context :AppContext) = {

        import sparkSession.sqlContext.implicits._

        val waysDF : Dataset[Row] = sparkSession.read.parquet(context.getOsmWaysFilePath)

        waysDF.cache()
        LOG.info("Number of all imported ways: %d".format(waysDF.count()))

        val defaultSpeed = 50.0
        val speedTag = "maxspeed"
        val oneWayTag = "oneway"
        val roundAboutTag = "junction"
        val roundAboutValue = "roundabout"
        val roadTag = "highway"
        val wayFiteredDF = waysDF.filter(array_contains($"tags.key", roadTag))
//        .where(($"id" === 26984518 || $"id" === 82222601 || $"id" === 138006028))
        
        
        val wayNodesDF = wayFiteredDF.select($"id".as("wayId"), $"tags", explode($"nodes").as("indexedNode"))
        .withColumn("linkId", monotonically_increasing_id())
            
        wayNodesDF.cache()
        val totalBidirectionalLinks = wayNodesDF.count()
            
        var nodeLinkJoinDF = nodeDF.join(wayNodesDF, $"indexedNode.nodeId" === nodeDF("id"))
        nodeLinkJoinDF.cache()
        var nodesInLinksDF = nodeLinkJoinDF.select($"id", $"latitude", $"longitude").dropDuplicates()
            
        val wayDF = nodeLinkJoinDF.groupBy($"wayId", $"tags")
        .agg(collect_list(struct($"indexedNode.index", $"indexedNode.nodeId", $"latitude", $"longitude")).as("nodes")
        , collect_list($"linkId").as("linkIds"))
                            
        var linkDS = wayDF.flatMap((row : Row) => {

            var links : List[Link] = List.empty[Link]
            var tagsMap = Map.empty[String, String]
            try {
                                
                var tags = row.getAs[WrappedArray[Row]](1)

                tagsMap = tags
                    .map(r => new String(r.getAs[Array[Byte]]("key")) -> new String(r.getAs[Array[Byte]]("value"))).toMap

                var speed = defaultSpeed
                val speedOption = tagsMap.get(speedTag)
                if (!speedOption.isEmpty) speed = speedOption.get.toInt

                val isOneWay = tagsMap.getOrElse(oneWayTag, "no") == "yes"
                val isRoundAbout = tagsMap.getOrElse(roundAboutTag, "default") == roundAboutValue

                speed = speed / 3.6

                var nodes = row.getAs[WrappedArray[Row]](2)
                    .map(r => (r.getInt(0), r.getAs[Long](1), r.getAs[Double](2), r.getAs[Double](3))).array
                    .sortBy(x => x._1)

                var linkIds = row.getAs[WrappedArray[Long]](3).toArray

                for (i <- 0 until nodes.length - 1) {
                    var tail = nodes(i)
                    var head = nodes(i + 1)

                    links = links :+ createLink(tail, head, linkIds(i), speed)

                    if (!isOneWay && !isRoundAbout) {
                        tail = nodes(i + 1)
                        head = nodes(i)
                        links = links :+ createLink(tail, head, linkIds(i) + totalBidirectionalLinks, speed)
                    }

                }

            } catch {
                case e : Exception => {
                    LOG.error("Error processing way %d. Tags: %s".format(row.getLong(0), tagsMap.mkString(" | ")), e)
                }
            }
            links
        })

        (nodesInLinksDF, linkDS)
    }
    
    
    private def createLink(tail : nodeType, head :nodeType, linkId :Long, speed :Double) :Link = {

        val tailLon = tail._3
        val tailLat = tail._4
        val headLon = head._3
        val headLat = head._4

        val length = GeometryUtils.getDistance(tailLon, tailLat, headLon, headLat)
        Link(linkId, tail._2, head._2, length.toFloat, speed.toFloat)
    }
    
    
    
    

}
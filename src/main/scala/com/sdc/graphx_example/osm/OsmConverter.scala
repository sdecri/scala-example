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
import org.apache.spark.storage.StorageLevel
import com.sdc.graphx_example.network.SimplePoint
import com.sdc.graphx_example.network.Node
import com.sdc.graphx_example.command_line.NETWORK_OUTPUT_FORMAT

/**
 * Class that import osm parquet produced with osm-parquetizer
 * <a href="http://wiki.openstreetmap.org/wiki/Osm-parquetizer">http://wiki.openstreetmap.org/wiki/Osm-parquetizer</a>
 * creating <code>Node</code> and <code>Link</code> dataframe.
 */
object OsmConverter {
    
    private type nodeType = (Int, Long, Double, Double)
    
    private val LOG = LoggerFactory.getLogger(getClass)
    


    def convertToNetwork[VD](sparkSession : SparkSession, context :AppContext) : Unit = {
        
        LOG.info("Convert OSM parquet to internal network parquet with context: %s".format(context))
        
        var allNodeDF = convertNodes(sparkSession, context)
        
        LOG.info("Number of all imported nodes: %d".format(allNodeDF.count()))
        
        var net = convertLinks(sparkSession, allNodeDF, context)
        
        val nodesOutputFilePath = context.getOutputDir + "nodes"
        var nodeDS = net._1
        if(context.getOsmConverterPersistNodes){
            if (context.getNodesRepartitionOutput > 0)
                nodeDS = nodeDS.repartition(context.getNodesRepartitionOutput)

            if (context.getNetworkOutputFormat == NETWORK_OUTPUT_FORMAT.CSV) {
                
                sparkSession.createDataFrame(nodeDS.rdd.map(_.toRow()), Node.SCHEMA_CSV)
                .write.options(Node.CSV_OPTIONS).mode(SaveMode.Overwrite)
                .csv(nodesOutputFilePath)
                
            }else
                nodeDS.write.mode(SaveMode.Overwrite).json(nodesOutputFilePath)
                
        }
        println("Number of network nodes: %d".format(nodeDS.count()))

        val linksOutputFilePath = context.getOutputDir + "links"
        var linkDS = net._2
        if(context.getOsmConverterPersistLinks) {
            if (context.getLinksRepartitionOutput > 0)
                linkDS = linkDS.repartition(context.getLinksRepartitionOutput)
            if(context.getNetworkOutputFormat == NETWORK_OUTPUT_FORMAT.CSV){
                
                sparkSession.createDataFrame(linkDS.rdd.map(_.toRow), Link.SCHEMA_CSV)
                .write.options(Link.CSV_OPTIONS).mode(SaveMode.Overwrite)
                .csv(linksOutputFilePath)
                
            }
            else
                linkDS.write.mode(SaveMode.Overwrite).json(linksOutputFilePath)
            
        }
        val linkCounter = net._3
        println("Number of network links: %d".format(linkCounter.count))

    }

    private def convertNodes(sparkSession : SparkSession, context :AppContext) : DataFrame = {
        val nodesOsmDF = sparkSession.read.parquet(context.getOsmNodesFilePath)
        nodesOsmDF.select("id", "latitude", "longitude")
    }

    private def convertLinks(sparkSession : SparkSession, nodeDF : DataFrame, context :AppContext) = {

        val waysDF : Dataset[Row] = sparkSession.read.parquet(context.getOsmWaysFilePath)
        LOG.info("Number of all imported ways: %d".format(waysDF.count()))

        val defaultSpeed = 50.0
        val speedTag = "maxspeed"
        val oneWayTag = "oneway"
        val roundAboutTag = "junction"
        val roundAboutValue = "roundabout"
        val roadTag = "highway"
        val wayFiteredDF = waysDF.filter(array_contains(col("tags.key"), roadTag))
//        .where(($"id" === 26984518 || $"id" === 82222601 || $"id" === 138006028))
        
        
        
        val wayNodesDF = wayFiteredDF.select(col("id").as("wayId"), col("tags"), explode(col("nodes")).as("indexedNode"))
        .withColumn("linkId", monotonically_increasing_id())
        
        val totalBidirectionalLinks = wayNodesDF.count()
            
        var nodeLinkJoinDF = nodeDF.join(wayNodesDF, col("indexedNode.nodeId") === nodeDF("id"))
        nodeLinkJoinDF.cache()
        var nodesInLinksDF = nodeLinkJoinDF.select(col("indexedNode.nodeId").as("id"), col("latitude"), col("longitude")).dropDuplicates()
            
        val wayDF = nodeLinkJoinDF.groupBy(col("wayId"), col("tags"))
        .agg(collect_list(struct(col("indexedNode.index"), col("indexedNode.nodeId"), col("latitude"), col("longitude"))).as("nodes")
        , collect_list(col("linkId")).as("linkIds"))
        
        val linkCounter = sparkSession.sparkContext.longAccumulator("linkCounter")
        
        
        
        
        var linkDF :Dataset[Link] =            
            wayDF.flatMap((row : Row) => {

            var links : List[Link] = List.empty[Link]
            var tagsMap = Map.empty[String, String]
            try {
                                
                var tags = row.getAs[WrappedArray[Row]](1)

                tagsMap = tags
                    .map(r => new String(r.getAs[Array[Byte]]("key")) -> new String(r.getAs[Array[Byte]]("value"))).toMap

                var speed = defaultSpeed
                val speedOption = tagsMap.get(speedTag)
                if (!speedOption.isEmpty) speed = speedOption.get.toDouble

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
                    linkCounter.add(1)
                    
                    if (!isOneWay && !isRoundAbout) {
                        tail = nodes(i + 1)
                        head = nodes(i)
                        links = links :+ createLink(tail, head, linkIds(i) + totalBidirectionalLinks, speed)
                        linkCounter.add(1)
                    }

                }

            } catch {
                case e : Exception => {
                    LOG.error("Error processing way %d. Tags: %s".format(row.getLong(0), tagsMap.mkString(" | ")), e)
                }
            }
            
            links
            
        })(Encoders.product[Link])
        
        linkDF.persist(StorageLevel.MEMORY_AND_DISK)
        
        // fill tail coordinates
        val linkWithTailDF = linkDF.alias("links").join(nodesInLinksDF.alias("nodes"), col("links.tail") === col("nodes.id"))
        .select(col("links.*"), col("nodes.longitude").as("tail_lon"), col("nodes.latitude").as("tail_lat"))
        
        val linkWithTailHeadDF = linkWithTailDF.alias("links").join(nodesInLinksDF.alias("nodes"), col("links.head") === col("nodes.id"))
        .select(col("links.*"), col("nodes.longitude").as("head_lon"), col("nodes.latitude").as("head_lat"))
        
        
        
        var linkDS = linkWithTailHeadDF.map((row :Row) => {
            
            val points = Array(SimplePoint(row.getDouble(6).toFloat, row.getDouble(7).toFloat)
                    , SimplePoint(row.getDouble(8).toFloat, row.getDouble(9).toFloat))
            
            val link = Link(row.getLong(0)
                    , row.getLong(1)
                    , row.getLong(2)
                    , row.getFloat(3)
                    , row.getFloat(4)
                    , points)
                    
            link
            
        })(Encoders.product[Link])
        
        var nodeDS = nodesInLinksDF.map((r:Row) => Node(r.getLong(0), SimplePoint(r.getDouble(2).toFloat, r.getDouble(1).toFloat)))(Encoders.product[Node])
        
        (nodeDS, linkDS, linkCounter)
    }
    
    
    private def createLink(tail : nodeType, head :nodeType, linkId :Long, speed :Double) :Link = {

        val tailLon = tail._3
        val tailLat = tail._4
        val headLon = head._3
        val headLat = head._4

        val length = GeometryUtils.getDistance(tailLon, tailLat, headLon, headLat)
        Link(linkId, tail._2, head._2, length.toFloat, speed.toFloat, null)
    }
    
    
    
    

}
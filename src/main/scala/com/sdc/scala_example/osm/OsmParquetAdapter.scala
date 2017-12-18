package com.sdc.scala_example.osm

import java.io.File
import org.apache.spark.sql._
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.functions._
import com.sdc.scala_example.network.Link
import scala.collection.mutable.WrappedArray
import java.util.Map
import com.sdc.scala_example.geometry.GeometryUtils
import org.slf4j.LoggerFactory

/**
 * Class that import osm parquet produced with osm-parquetizer
 * <a href="http://wiki.openstreetmap.org/wiki/Osm-parquetizer">http://wiki.openstreetmap.org/wiki/Osm-parquetizer</a>
 * creating <code>Node</code> and <code>Link</code> dataframe.
 */
object OsmParquetAdapter {
    
    val LOG = LoggerFactory.getLogger(OsmParquetAdapter.getClass)
    
    case class Context(nodesFile : File, waysFile : File, outputDir : String
            , nodesRepartition :Int = -1, linksRepartition :Int = -1){
        
        override def toString() :String = "NODES = %s, WAYS = %s, OUTPUT_DIR = %s, NODES_REPARTITION = %d, LINKS_REPARTITOIN = %d"
        .format(nodesFile,waysFile,outputDir,nodesRepartition,linksRepartition)
    }
    

    def convertToNetwork[VD](sparkSession : SparkSession, context :Context) : Unit = {

        LOG.info("Convert OSM parquet to internal network parquet with context: %s".format(context))
        
        var allNodeDF = convertNodes(sparkSession, context)
        allNodeDF.cache()
        
        LOG.info("Number of all imported nodes: %d".format(allNodeDF.count()))
        
        var net = convertLinks(sparkSession, allNodeDF, context)
        
        val nodesParquetFilePath = context.outputDir + "nodes"
        var nodeDF = net._1
        if(context.nodesRepartition > 0)
            nodeDF = nodeDF.repartition(context.nodesRepartition)
        nodeDF.write.mode(SaveMode.Overwrite).parquet(nodesParquetFilePath)
        
        LOG.info("Number of network nodes: %d".format(nodeDF.count()))
        
        val linksParquetFilePath = context.outputDir + "links"
        var linkDS = net._2
        linkDS.cache()
        if(context.linksRepartition > 0)
            linkDS = linkDS.repartition(context.linksRepartition)
        linkDS.write.mode(SaveMode.Overwrite).parquet(linksParquetFilePath)
        LOG.info("Number of network links: %d".format(linkDS.count()))

    }

    def convertNodes(sparkSession : org.apache.spark.sql.SparkSession, context :Context) : DataFrame = {
        val nodesOsmDF = sparkSession.read.parquet(context.nodesFile.getAbsolutePath)
        nodesOsmDF.select("id", "latitude", "longitude")
    }

    def convertLinks(sparkSession : org.apache.spark.sql.SparkSession, nodeDF : DataFrame, context :Context) = {

        val sqlContext = new SQLContext(sparkSession.sparkContext)
        import sqlContext.implicits._

        val waysDF : Dataset[Row] = sparkSession.read.parquet(context.waysFile.getAbsolutePath)

        val defaultSpeed = 50.0
        val speedTag = "maxspeed"
        val roadTag = "highway"
        val wayNodesDF = waysDF.filter(array_contains($"tags.key", roadTag))
            .select($"id".as("wayId"), $"tags", explode($"nodes").as("indexedNode"))
            .withColumn("linkId", monotonically_increasing_id())
            
        var nodeLinkJoinDF = nodeDF.join(wayNodesDF, $"indexedNode.nodeId" === nodeDF("id"))
        nodeLinkJoinDF.cache()
        var nodesInLinksDF = nodeLinkJoinDF.select($"id", $"latitude", $"longitude")
            
        val wayDF = nodeLinkJoinDF.groupBy($"wayId", $"tags")
            .agg(collect_list(struct($"indexedNode.index", $"indexedNode.nodeId", $"latitude", $"longitude")).as("nodes")
                    , collect_list($"linkId").as("linkIds"))

        var linkDS = wayDF.flatMap((row : Row) => {

            var tags = row.getAs[WrappedArray[Row]](1)

            val tagsMap = tags
                .map(r => new String(r.getAs[Array[Byte]]("key")) -> new String(r.getAs[Array[Byte]]("value"))).toMap

            var speed = defaultSpeed
            val speedOption = tagsMap.get(speedTag)
            if (!speedOption.isEmpty) speed = speedOption.get.toInt

            speed = speed / 3.6

            var nodes = row.getAs[WrappedArray[Row]](2).map(r => (r.getAs[Long](1), r.getAs[Double](2), r.getAs[Double](3))).array
            var linkIds = row.getAs[WrappedArray[Long]](3).toArray
            var links : List[Link] = List.empty[Link]

            for (i <- 0 until nodes.length - 1) {
                val tail = nodes(i)
                val head = nodes(i + 1)

                val tailLon = tail._2
                val tailLat = tail._3
                val headLon = head._2
                val headLat = head._3

                var length = GeometryUtils.getDistance(tailLon, tailLat, headLon, headLat)

                links = links :+ Link(linkIds(i), tail._1, head._1, length.toFloat, speed.toFloat)
            }

            links
        })

        (nodesInLinksDF, linkDS)
    }

}
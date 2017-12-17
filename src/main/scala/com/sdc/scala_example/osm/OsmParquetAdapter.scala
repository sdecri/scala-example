package com.sdc.scala_example.osm

import java.io.File
import org.apache.spark.sql._
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.functions._
import com.sdc.scala_example.network.Link
import scala.collection.mutable.WrappedArray
import java.util.Map


/**
 * Class that import osm parquet produced with osm-parquetizer
 * <a href="http://wiki.openstreetmap.org/wiki/Osm-parquetizer">http://wiki.openstreetmap.org/wiki/Osm-parquetizer</a>
 * creating <code>Node</code> and <code>Link</code> dataframe.
 */
object OsmParquetAdapter {

    def convertToNetwork[VD](sparkSession: SparkSession, nodesFile: File
            , waysFile :File, outputDir :String): Unit = {

        val sqlContext = new SQLContext(sparkSession.sparkContext)
        import sqlContext.implicits._
        
        val nodesOsmDF = sparkSession.read.parquet(nodesFile.getAbsolutePath)
        
        var nodeParquetFilePath = outputDir + "nodes.parquet"
        val nodesDF = nodesOsmDF.select("id","latitude", "longitude")
//        nodesDF.write.mode(SaveMode.Overwrite).parquet(nodeParquetFilePath)
        
        val waysDF : Dataset[Row] = sparkSession.read.parquet(waysFile.getAbsolutePath)
        waysDF.printSchema()
        waysDF.show(10, false)
        
        val defaultSpeed = 50
        val speedTag = "maxspeed"
        val roadTag = "highway"
        val wayNodesDF = waysDF.filter(array_contains($"tags.key", roadTag))
        .select($"id".as("wayId"), $"tags", explode($"nodes").as("indexedNode"))
        .withColumn("linkId", monotonically_increasing_id())
        
        val wayDF = nodesDF.join(wayNodesDF, $"indexedNode.nodeId" === nodesDF("id"))
        .groupBy($"wayId",$"tags")
        .agg(collect_list(struct($"indexedNode.index", $"indexedNode.nodeId", $"latitude", $"longitude")).as("nodes")
                ,collect_list($"linkId").as("linkIds"))

        wayDF.flatMap((row: Row) => {

            var tags = row.getAs[WrappedArray[Row]](1)

            val tagsMap = tags
                .map(r => new String(r.getAs[Array[Byte]]("key")) -> new String(r.getAs[Array[Byte]]("value"))).toMap

            var speed = defaultSpeed
            val speedOption = tagsMap.get(speedTag)
            if (!speedOption.isEmpty)
                speed = speedOption.get.toInt

            var nodes = row.getAs[WrappedArray[Row]](2).map(r => (r.getAs[Long](1), 0.0, 0.0)).array
            var linkIds = row.getAs[WrappedArray[Long]](3).toArray
            var links: List[Link] = List.empty[Link]
            var tail = 0
            var head = 0
            for (i <- 0 until nodes.length - 1) {
                tail = i
                head = i+1
                links = links :+ Link(linkIds(i), nodes(tail)._1, nodes(head)._1, 0, 0)
            }

            links
        })
        .show()

        
    }

}
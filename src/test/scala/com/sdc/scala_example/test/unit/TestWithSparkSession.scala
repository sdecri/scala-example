package com.sdc.scala_example.test.unit

import org.apache.spark.sql.SparkSession
import org.junit.Before
import org.junit.After
import java.io.File

class TestWithSparkSession {
    
    private var spark :SparkSession = _

    def getSpark() :SparkSession = spark
    
    
	@Before
	def setUp(){
        
        getOrCreateSparkSession()
	}

    def getOrCreateSparkSession() :SparkSession = {
        spark = SparkSession.builder().master("local").appName("Test")
				.config("driverMemory", "1G")
				.config("executorMemory", "6G")				
				// .config("spark.eventLog.enabled", "true")
				// .config("spark.eventLog.dir", "file:///C:/tmp/spark-log/spark-events")
				.getOrCreate();
        spark
    }
    
	@After
	def cleanUp(){
		if (spark != null) spark.close()
	}

    def deleteDirectory(dir : String) :Boolean= {
        val directory = new File(dir)
        if (directory.exists()){
            val files = directory.listFiles()
            for (file <- files) {
                if (file.isDirectory())
                    deleteDirectory(file.getAbsolutePath)
                else
                    file.delete()
            }
        }
        directory.delete()
    }
    
    
}
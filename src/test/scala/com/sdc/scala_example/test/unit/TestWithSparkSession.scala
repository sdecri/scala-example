package com.sdc.scala_example.test.unit

import org.apache.spark.sql.SparkSession
import org.junit.Before
import org.junit.After

class TestWithSparkSession {
    
    private var spark :SparkSession = _

    def getSpark() :SparkSession = spark
    
    
	@Before
	def setUp(){
        
        spark = SparkSession.builder().master("local").appName("Test")
				.config("driverMemory", "1G")
				.config("executorMemory", "6G")				
				// .config("spark.eventLog.enabled", "true")
				// .config("spark.eventLog.dir", "file:///C:/tmp/spark-log/spark-events")
				.getOrCreate();
	}

	@After
	def cleanUp(){
		if (spark != null) spark.close()
	}


    
    
}
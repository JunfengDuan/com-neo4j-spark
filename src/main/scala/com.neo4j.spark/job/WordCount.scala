package com.neo4j.spark.job

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
  * Created by jfd on 5/23/17.
  */
class WordCount {

    val logger = LoggerFactory.getLogger(this.getClass())
    logger.info("This is word count")

    doCount

    def doCount(): Unit ={

        val conf = new SparkConf().setAppName("WorldCount Application").setMaster("local")
        val sc = new SparkContext(conf)

        val lines: RDD[String] = sc.textFile("/home/jfd/soft/spark-2.1.0-bin-hadoop2.7/README.md")
        val words = lines.flatMap(_.split(" "))

        val pairs = words.map{word => (word,1)}
        val wordCounts = pairs.reduceByKey(_ + _)

        wordCounts.foreach(wordNumberPair => println(wordNumberPair._1+" : "+wordNumberPair._2))

        sc.stop()
    }

}

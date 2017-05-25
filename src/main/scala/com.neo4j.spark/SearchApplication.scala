package com.neo4j.spark

import com.neo4j.spark.job.PageRank
import org.springframework.boot.SpringApplication


object SearchApplication extends App{

    SpringApplication.run(classOf[PageRank]);
//    SpringApplication.run(classOf[WordCount]);

}

package com.neo4j.spark.job

import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.lib.PageRank
import org.apache.spark.{SparkConf, SparkContext}
import org.neo4j.spark._
import org.slf4j.LoggerFactory

class PageRank{

    val logger = LoggerFactory.getLogger(this.getClass())
    logger.info("This is page rank")

    val conf = new SparkConf().setAppName("PageRank Application").setMaster("local")
        .set("spark.neo4j.bolt.password","Neo4j").setJars(Array("libs/neo4j-spark-connector-2.0.0-M2.jar"))

    val sc = new SparkContext(conf)
    val neo = Neo4j(sc)

    loadGraph(neo)

    def loadGraph(neo: Neo4j){

//        val rdd = neo.cypher("MATCH (n:rs01) RETURN n").loadNodeRdds
//        logger.debug("nodes count:{}",rdd.count)

        // load graph via pattern
//        val graph = neo.pattern(("rs01","recordid"),("RS07","recordid"),("rs06","recordid")).partitions(3).batch(200).loadGraph[String,String]

        val graph = neo.pattern(("Person","id"),("KNOWS","since"),("Person","id")).partitions(7).batch(200).loadGraph[Long,Double]

        logger.debug("graph.vertices count:{}",graph.vertices.count)
        logger.debug("graph.edges count:{}",graph.edges.count)

        doRank(graph)

    }



    def doRank(graph: Graph[Long,Double]){

        val graph2 = PageRank.run(graph, 5)

        val array = graph2.vertices.take(5)
        logger.debug("Graph with rank prop:{}",array)


        // uses pattern from above to save the data, merge parameter is false by default, only update existing nodes
        val num = neo.saveGraph(graph2, nodeProp = "rank")
        logger.debug("Updated nodes num:{}",num)



    }

    def loadGraphViaCypher(neo: Neo4j){
        val graphQuery = "MATCH (n:rs01)-[r:RS07]->(m:rs06) RETURN id(n) as source, id(m) as target, type(r) as value SKIP {_skip} LIMIT {_limit}"
        val graph: Graph[Long, String] = neo.rels(graphQuery).partitions(7).batch(200).loadGraph

        graph.vertices.count
        graph.edges.count

    }

}



// Databricks notebook source exported at Sat, 5 Nov 2016 03:55:12 UTC
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.IntParam
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx.lib.TriangleCount



//Part 1:
case class CoAuthors(Author1:Long, Author2:Long)
def parseCoAuthors(str: String): CoAuthors = {
 val line = str.split("\t")
 CoAuthors(line(0).toLong , line(1).toLong)
}
val textRDD = sc.textFile("/FileStore/tables/4fwv0tm31478312019711/CA_HepTh-9cc05.txt")
val coauthorsRDD = textRDD.map(parseCoAuthors).cache()

//Part 2:
//Vertex description
coauthorsRDD.take(1)
val nowhere = "nowhere"
val authors = coauthorsRDD.map(CoAuthors => (CoAuthors.Author1)).distinct
val authorMap = authors.collect.toList

//Edge description
val collaborators = coauthorsRDD.map(coauthor => ((coauthor.Author1, coauthor.Author2), 1))
collaborators.take(2)
val edges = collaborators.map {
 case ((author1, author2), distance) =>Edge(author1.toLong, author2.toLong, 1) }
edges.take(1)

//Property Graph Definition
val graph = Graph.fromEdges(edges, nowhere)
graph.edges.take(2)
val numauthors = graph.numVertices
val numcollaborators = graph.numEdges

//Part 3a:
def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
 if (a._2 > b._2) a else b
}
val maxOutDegree: (VertexId, Int) = graph.outDegrees.reduce(max)

//Part 3b:
val maxInDegree: (VertexId, Int) = graph.inDegrees.reduce(max)

//Part 3c:
//Threshold = 0.01
val ranks = graph.pageRank(0.01).vertices
ranks.take(1)
//ordering by descending order according to rank and taking top 5
val highestRank = ranks.sortBy(_._2, false)
highestRank.take(5)

//Part 3d:
val cc = graph.connectedComponents().vertices
cc.collect()

//Part 3e:
val triCount = TriangleCount.runPreCanonicalized(graph)
triCount.vertices.sortBy(_._2,false).take(5)

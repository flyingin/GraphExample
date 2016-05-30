package Scala.Dijkstra

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer


/**
  * Created by yml on 16/5/20.
  */
object DijkstraScala {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("DijkstraScala").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val FileRdd = sc.textFile("input/case1.csv")

    //顶点的构造
    val users:RDD[(VertexId, Double)] = FileRdd.mapPartitions(records=>{
      val arr = ArrayBuffer[(Long,(Double))]()
      for(record <- records){
        val contains = record.split(",")
        arr += ((contains(1).toLong,contains(1).toDouble))
      }
      arr.toIterator
    }).distinct()

    /*在构造图是*/
    //边的构造
    val relationships :RDD[Edge[Double]] = FileRdd.mapPartitions(records=> {
      val arr = ArrayBuffer[Edge[Double]]()
      for (record <- records) {
        val contains = record.split(",")
        val x = Edge(contains(1).toInt, contains(2).toInt, contains(3).toDouble)
        arr += x
      }
      arr.toIterator
    })
    //构造图
    val graph = Graph(users,relationships)

    /*println("graph:")
    println("vertices:")
    graph.vertices.collect.foreach(println)
    println("edges:")
    graph.edges.collect.foreach(println)*/

    val StartId : VertexId = 0
    val initialGraph = graph.mapVertices((id, _) => if (id == StartId) 0.0 else Double.PositiveInfinity)

    /*println("initialGraph:")
    println("vertices:")
    initialGraph.vertices.collect.foreach(println)
    println("edges:")
    initialGraph.edges.collect.foreach(println)*/

    val sssp = initialGraph.pregel(Double.PositiveInfinity)(
      // Vertex Program，节点处理消息的函数，dist为原节点属性（Double），newDist为消息类型（Double）
      vprog = (id, dist, newDist) => {math.min(dist, newDist)},

      // Send Message，发送消息函数，返回结果为（目标节点id，消息（即最短距离））
      sendMsg = triplet => {
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      //Merge Message，对消息进行合并的操作，类似于Hadoop中的combiner
      mergeMsg = (a, b) => math.min(a, b)
    )

    val x = graph.mapVertices((id, _) => if(id ==StartId) (List(StartId.toLong),0.0) else (List(StartId.toLong),Double.PositiveInfinity))
    /*-----这里也可以是List[VertexId]()*/
    val y = x.pregel((List(StartId),Double.PositiveInfinity))(
      vprog = (id,dist,newDist) =>{
        if(dist._2<newDist._2){
          dist
        }else
          newDist
      },
      sendMsg = triplet =>{
        if(triplet.srcAttr._2+triplet.attr < triplet.dstAttr._2){
          val r = triplet.srcAttr._2 + triplet.attr
          val l = triplet.srcAttr._1 :+ triplet.dstId
          Iterator((triplet.dstId,(l,r)))
        }else{
          Iterator.empty
        }
      },
      mergeMsg = (a,b) =>{
        if(a._2<b._2){
          a
        }
        else b
      }
    )
    val r = y.vertices.map(vertex => StartId+"to "+vertex._1+"distance is "+vertex._2._2+"path is "+vertex._2._1)
    r.foreach(println)
    /*println("sssp:")
    println("vertices:")
    sssp.vertices.collect.foreach(println)
    println("edges:")
    sssp.edges.collect.foreach(println)

    val result = sssp.vertices.map(vertex =>
      "Vertex " + vertex._1 + ": distance is " + vertex._2 )
    result.foreach(println)*/
    sc.stop()
  }

}

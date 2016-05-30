package Scala
import java.io.{File, PrintWriter}

import scala.io.Source
import scala.util.Random
/**
  * Created by yml on 16/5/30.
  */
object File {
  def main(args: Array[String]) {
    val file = Source.fromFile("/Users/yml/Downloads/web-Google.txt")
    val writer = new PrintWriter(new File("input/case4.csv"))
    var count = 0
    for(line <- file.getLines()){
      val record = line.split("\t")
      val str = count+","+record(0)+","+record(1)+","+Random.nextInt(15)
      writer.println(str)
      count = count+1
    }
    writer.close()
    file.close()
    System.currentTimeMillis()
  }
}

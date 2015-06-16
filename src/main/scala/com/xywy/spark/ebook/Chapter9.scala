package com.xywy.spark.ebook

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Advanced Text Processing with Spark
 * Created by 49236_000 on 2015/6/16.
 */
class Chapter9 {

}
object Chapter9{
  val conf=new SparkConf()
    .setAppName("Chapter9")
    .setMaster("spark://slave4:7077")

  val sc=new SparkContext()
  val path="file:////home/hadoop/data/20Newsgroups/20news-bydate-train/*"
  val rdd=sc.wholeTextFiles(path)

  val text=rdd.map{
    case(file,text) => text
  }
  println(text.count())

  val newsgroups=rdd.map{
    case (file,text)=>
      file.split("/").takeRight(2).head
  }
  val countByGroup=newsgroups.map(n=>(n,1)).reduceByKey(_+_).collect().sortBy(-_._2).mkString("\n")
  println(countByGroup)

  //aplying basic tolenization
//  val text=rdd.map{
//    case (file,text) => text
//  }
  val whiteSpaceSplit=text.flatMap(t => t.split(" ").map(_.toLowerCase))
  println(whiteSpaceSplit.distinct().count())

  //test
  println(whiteSpaceSplit.sample(true,0.3,42).take(100).mkString(","))

  //improving our tokenization
  val nonWordSplit=text.flatMap(t=> t.split("""\W+""").map(_.toLowerCase()))
  println(nonWordSplit.distinct().count())

  //test
  println(nonWordSplit.distinct().sample(true,0.3,42).take(100).mkString(","))
}

package com.xywy.spark.oreilly

import org.apache.spark.{SparkContext, SparkConf}

/**
 * advance analysis with spark
 * Created by xiaoni on 2015/5/13.
 */
object Chapter1 {
    def main(args: Array[String]): Unit ={
      val conf=new SparkConf()
      .setAppName("Chapter1")
      .setMaster("spark://slave4:7077")
      val sc=new SparkContext(conf)
      val data=sc.parallelize(List(1,2,3,4))
      println(data.collect)
    }
}

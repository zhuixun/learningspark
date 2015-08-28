package com.xywy.spark.ebook

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vector

/**
 * advance analysis with spark
 * LG 回归分类
 * Created by xiaoni on 2015/5/13.
 */
class Chapter5{
  //去除数据head数据
  def noHeader(rawData:RDD[Array[String]]):RDD[Array[String]]={
    rawData.filter(line=> line.contains("urlid"))
  }
}
object Chapter5 {
    def main(args: Array[String]): Unit ={
      //conf配置
      val conf=new SparkConf()
      .setAppName("Chapter5")
      .setMaster("spark://slave4:7077")

      val chapter=new Chapter5
      val sc=new SparkContext(conf)
//      System.setProperty("hadoop.home.dir", "D:\\Hadoop\\hadoop-2.2.0\\bin")
      //读取数据
      val trainData=sc.textFile("file:///home/niweiwei/work/kd/train.tsv")
//      val trainData=sc.textFile("D:\\Data\\kaggle\\train.tsv")
      //对文件split
      val rawData=trainData.map(line => line.split("\t"))
      //去除文件头
      val records=chapter.noHeader(rawData)
















//      println(rawData.first())
      sc.stop()


    }
}

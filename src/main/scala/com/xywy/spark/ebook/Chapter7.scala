package com.xywy.spark.ebook

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.clustering.KMeans
import breeze.linalg._
import breeze.numerics.pow

/**
 * k-means 聚类
 * Created by 49236_000 on 2015/5/17.
 */
class Chapter7 {

}
object Chapter7{
  def main(args:Array[String]): Unit ={
    //conf配置
    val conf=new SparkConf()
      .setAppName("Chapter5")
      .setMaster("spark://slave4:7077")

    val sc=new SparkContext(conf)
    //电影数据
    val movies=sc.textFile("file:///home/hadoop/u.item")
    //genres数据
    val genres=sc.textFile("file:///home/hadoop/u.genre")
    //转换成 Map(1 -> Action)
    val genreMap=genres.filter(!_.isEmpty).map(_.split("\\|"))
    .map(array=>(array(1),array(0))).collectAsMap()

    val titlesAndGenres=movies.map(_.split("\\|")).map{ array =>
      val genres=array.toSeq.slice(5,array.size)
      val genresAssigned=genres.zipWithIndex.filter {
        case (g, idx) =>
          g == "1"
      }.map{
        case (g,idx)  =>
          genreMap(idx.toString)
      }
      (array(0).toInt,(array(1),genresAssigned))
    }
    println(titlesAndGenres.first())


    //training the recommendaion model
    val rawData=sc.textFile("file:///u.data")
    val rawRatings=rawData.map(_.split("\t").take(3))
    val ratings=rawRatings.map{case Array(user,movie,rating)=>
      Rating(user.toInt,movie.toInt,rating.toDouble)
    }
    ratings.cache()
    val alsModel=ALS.train(ratings,50,10,0.1)

    val movieFactors=alsModel.productFeatures.map{
      case(id,factor) =>
        (id,Vectors.dense(factor))
    }
    val movieVectors=movieFactors.map(_._2)
    val userFactors=alsModel.userFeatures.map{
      case(id,factor) =>
        (id,Vectors.dense(factor))
    }
    val userVectors=userFactors.map(_._2)

    //Normallization
    val movieMatrix=new RowMatrix((movieVectors))
    val movieMatrixSummary=movieMatrix.computeColumnSummaryStatistics()
    val userMatrix=new RowMatrix(userVectors)
    val userMatrixSummary=userMatrix.computeColumnSummaryStatistics()

    println("Movie factors mean: " + movieMatrixSummary.mean)
    println("Movie factors variance: " + movieMatrixSummary.variance)
    println("User factors mean: " + userMatrixSummary.mean)
    println("User factors variance: " + userMatrixSummary.variance)

    //Training a clustering model
    val numClusters=5
    val numIterations=10
    val numRuns=3
    val movieClusterModel=KMeans.train(movieVectors,numClusters,numIterations,numRuns)
//    val movieClusterModelConverged=KMeans.train(movieVectors,numClusters,100)

    val userClusterModel=KMeans.train(userVectors,numClusters,numIterations,numRuns)
    //predictions
    val movie1=movieVectors.first()
    val movieCluster=movieClusterModel.predict(movie1)
    println(movieCluster)

    val predictions=movieClusterModel.predict(movieVectors)
    println(predictions.take(10).mkString(","))

    //计算距离
    def computerDistance(v1: DenseVector[Double],v2: DenseVector[Double])=pow(v1-v2,2).sum

  }
}

package com.xywy.spark.ebook

import  org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
import org.jblas.DoubleMatrix
import org.apache.log4j.{Logger,Level}

/**
 * Created by xiaoni on 2015/8/25.
 *
 */
class Chapter4{

}

object Chapter4 {
  def main(args: Array[String]): Unit ={
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val conf = new SparkConf()
    .setAppName("Chapter4")
    .setMaster("spark://namenode:7077")

    val sc=new SparkContext()

    //读取用户数据
    val rawData=sc.textFile("file:///home/hadoop/data/movielens/ml-100k/u.data")

    //格式：user、movie、rating
    val rawRatings=rawData.map(_.split("\t").take(3))

    //rating
    val rating=rawRatings.map{case Array(user, movie, rating) => Rating(user.toInt , movie.toInt ,rating.toDouble)}

    //训练模型
    val model=ALS.train(rating,50,10,0.01)

    //预测用户789
    val predictedRating=model.predict(789,123)

    //取前10条数据
    val topKRecs=model.recommendProducts(789,10)
//    println(topKRecs.mkString("\n"))

    //读取电影数据
    val movies=sc.textFile("file:///home/hadoop/data/movielens/ml-100k/u.item")
    val titles=movies.map(line => line.split("\\|").take(2)).map(array => (array(0).toInt,array(1))).collectAsMap()

    val moviesForUser=rating.keyBy(_.user).lookup(789)
//    println(moviesForUser.size)

//    moviesForUser.sortBy(-_.rating).take(10).map(rating => (titles(rating.product),rating.rating)).foreach(println)
//    topKRecs.map(rating => (titles(rating.product),rating.rating)).foreach(println)

    val aMatrix = new DoubleMatrix(Array(1.0, 2.0, 3.0))

    //计算余弦相似度
    def cosineSimilarity(vec1: DoubleMatrix, vec2: DoubleMatrix): Double =
    {
      vec1.dot(vec2) / (vec1.norm2() * vec2.norm2())
    }

    val itemId=567
    val itemFactor = model.productFeatures.lookup(itemId).head
    val itemVector = new DoubleMatrix(itemFactor)
    println(cosineSimilarity(itemVector,itemVector))
  }
}

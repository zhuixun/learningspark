package com.xywy.spark.ebook

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.linalg.{ SparseVector => SV }
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.feature.IDF
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

  //regex
  val regex="""[^0-9]*""".r
  val filterNumbers=nonWordSplit.filter(token=>
  regex.pattern.matcher(token).matches()
  )
  println(filterNumbers.distinct().count())
  println(filterNumbers.distinct.sample(true, 0.3, 42).take(100).mkString(","))

  //Removing stop words
  val tokenCounts = filterNumbers.map(t => (t, 1)).reduceByKey(_ + _)
  val oreringDesc=Ordering.by[(String,Int),Int](_._2)
  println(tokenCounts.top(20)(oreringDesc).mkString("\n"))

  val stopWords=Set("the","a","an","of","or","in","for","by","on","but", "is", "not","with", "as", "was", "if","they", "are", "this", "and", "it", "have", "from", "at", "my","be", "that", "to")
  val tokenCountsFilteredStopwords=tokenCounts.filter{ case
    (k,v) => !stopWords.contains(k)
  }
//  println(tokenCountsFilteredStopwords.top(20)(oreringDesc).mkString("\n"))

  val tokenCountsFilteredSize=tokenCountsFilteredStopwords.filter{
    case(k,v) => k.size >= 2
  }
  println(tokenCountsFilteredSize.top(20)(oreringDesc).mkString("\n"))

  //Excluding terms based on frequency
  val oreringAsc=Ordering.by[(String,Int),Int](_._2)
  println(tokenCountsFilteredSize.top(20)(oreringAsc).mkString("\n"))

  val rareTokens=tokenCounts.filter{
    case (k,v) => v<2
  }.map{
    case(k,v) =>k
  }.collect.toSet

  val tokenCountsFilteredAll=tokenCountsFilteredSize.filter{
    case (k,v) => !rareTokens.contains(k)
  }
  println(tokenCountsFilteredAll.top(20)(oreringAsc).mkString("\n"))

  println(tokenCountsFilteredAll.count)

  def tokenize(line: String): Seq[String] = {
    line.split("""\W+""")
      .map(_.toLowerCase)
      .filter(token => regex.pattern.matcher(token).matches)
      .filterNot(token => stopWords.contains(token))
      .filterNot(token => rareTokens.contains(token))
      .filter(token => token.size >= 2)
      .toSeq
  }

  val tokens=text.map(doc=>tokenize(doc))
  println(tokens.first.take(20))

  //Training a TF-IDF model
  val dim=math.pow(2,18).toInt
  val hashingTF=new HashingTF(dim)

  val tf=hashingTF.transform(tokens)
  tf.cache()

  val v = tf.first.asInstanceOf[SV]
  println(v.size)
  println(v.values.size)
  println(v.values.take(10).toSeq)
  println(v.indices.take(10).toSeq)
}

package com.github.mrpowers.my.cool.project

object JsonReader extends App {
  import org.apache.spark.{SparkConf, SparkContext}
  import org.json4s.DefaultFormats
  import org.json4s.native.JsonMethods.parse

  implicit val formats: DefaultFormats.type = DefaultFormats
  val path = args(0)

  case class json_class(id:String="", country:String="", points:String="", price:String="", title:String="", variety:String="", winery:String="")
  val conf = new SparkConf().setAppName("appName")
  val sc = new SparkContext(conf)

  val decodedUser = sc.textFile(path).map(js => parse(js).extract[json_class])

  decodedUser.take(10).foreach(println)
}

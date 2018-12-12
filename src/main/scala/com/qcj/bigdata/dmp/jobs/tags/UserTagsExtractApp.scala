package com.qcj.bigdata.dmp.jobs.tags

import com.qcj.bigdata.dmp.tags.{AdPositionTags, AppNameTags, ChannelTags}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
  * 用户标签提取应用
  */
/* 步骤：
  *  创建字典映射表：
  */
object UserTagsExtractApp {
  def main(args: Array[String]): Unit = {
    /*if(args == null || args.length < 5) {
      println(
        """Parameter Errors! Usage: <appmapping> <devicemapping> <userTable> <adlogs> <target>
          |appmapping      :   appmapping
          |devicemapping   :   devicemapping
          |userTable       ：  userTable
          |adlogs          :   adlogs
          |target          ：  target
        """.stripMargin)
      System.exit(-1)
    }
    val Array(appmapping, devicemapping, userTable, adlogs, target) = args*/

    val conf = new SparkConf()
      .setAppName("UserTagsExtractApp")
      .setMaster("local[2]")

    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    //加载广告数据
    //val adLogsDF = spark.read.parquet(adlogs)
    val adLogsDF = spark.read.parquet("data/out/")
    adLogsDF.show()
    //用户对应的多个标签
    val userid2TagsRDD:RDD[(String, mutable.Map[String, Int])] = adLogsDF.rdd.map{case row => {
        //在这里面完成的标签的提取
        val userId = row.getAs[String]("userid")//平台用户id
        //返回标签规定好的格式数据：LC_0->1
        //1.广告位标签
        val positionTags:mutable.Map[String, Int] = AdPositionTags.extractTag(row)//根据广告位类型（adspacetype）返回元组
        //2.APP标签
        val appNameTags:mutable.Map[String, Int] = AppNameTags.extractTag(row)
        //3.渠道标签
        val channelTags:mutable.Map[String, Int] = ChannelTags.extractTag(row)

      //将所有用户对应的标签MAP合并(标签1->1,标签2->2,标签3->3)
      val tagsMap = positionTags.++(appNameTags).++(channelTags)
      println(s"-----------------${userId}")
      println(s"-----------------${row.fieldIndex("userid")}")
      (userId, tagsMap)//用户：标签
    }}
    //将标签进行合并
    //wordcount----->reduceByKey() <user, map>
    val useridTagsRDD:RDD[(String, mutable.Map[String, Int])] = userid2TagsRDD.reduceByKey{case (map1, map2) => {
      for((tag, count) <- map2) {//第二个中的
        map1.put(tag, map1.getOrElse(tag, 0) + count)//map1中的值加上map2中的值相加之后放入map1中
        //                val map1OldValue = map1.get(tag)
        //                map1.put(tag, count + map1OldValue.getOrElse(0))
      }
      map1
    }}

    useridTagsRDD.foreach(println)
    spark.stop()
  }
}

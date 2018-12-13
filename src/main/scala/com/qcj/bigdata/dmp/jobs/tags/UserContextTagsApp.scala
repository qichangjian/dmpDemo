package com.qcj.bigdata.dmp.jobs.tags

import java.util.Properties

import com.qcj.bigdata.dmp.tags._
import com.qcj.bigdata.dmp.util.Utils
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable

/**
  * 标签的合并
  * 将数据库中的用户信息与标签的合集join得到一个完整的用户返回信息
  */
object UserContextTagsApp {
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
    //adLogsDF.show()


    //用户对应的多个标签
    val userid2TagsRDD:RDD[(String, mutable.Map[String, Int])] = adLogsDF.rdd.map{case row => {
        //在这里面完成的标签的提取
//        val userId = row.getAs[String]("userid")//平台用户id
      //当前用户的唯一的标识
      val userId = getNotEmptyID(row).getOrElse("UnKnow-User")

        //返回标签规定好的格式数据：LC_0->1
        //1.广告位标签
        val positionTags:mutable.Map[String, Int] = AdPositionTags.extractTag(row)//根据广告位类型（adspacetype）返回元组
        //2.APP标签
        val appNameTags:mutable.Map[String, Int] = AppNameTags.extractTag(row)
        //3.渠道标签
        val channelTags:mutable.Map[String, Int] = ChannelTags.extractTag(row)
        //4.设备：操作系统|联网方式|运营商  标签
        val deviceTags:mutable.Map[String, Int]  = DeviceTags.extractTag(row)
        //5.关键字标签
        val keywordTags:mutable.Map[String, Int] = KeyWordTags.extractTag(row)
        //6.地域标签
        val zoneTags:mutable.Map[String, Int] = ZoneTags.extractTag(row)
      //将所有用户对应的标签MAP合并(标签1->1,标签2->2,标签3->3)
//      val tagsMap = positionTags.++(appNameTags).++(channelTags)
      val tagsMap = Utils.addTags(positionTags, appNameTags, channelTags, deviceTags, keywordTags, zoneTags)
      (userId, tagsMap)//用户：标签
    }}
    //(1,Map(ZC_上海市 -> 1, APP_马上赚 -> 1, ZP_上海市 -> 1, DEVICE_NETWORK_D0002001 -> 1, LC_02 -> 1, DEVICE_ISP_D0003004 -> 1, DEVICE_OS_D0001001 -> 1, CN_ -> 0))
//    userid2TagsRDD.foreach(println)


    //将标签进行合并
    //wordcount----->reduceByKey() <user, map>
    val useridTagsRDD:RDD[(String, mutable.Map[String, Int])] = userid2TagsRDD.reduceByKey{case (map1, map2) => {
      for((tag, count) <- map2) {//第二个中的
        map1.put(tag, map1.getOrElse(tag, 0) + count)//map1中的值加上map2中的值相加之后放入map1中
      }
      map1
    }}
    //(2,Map(LC_02 -> 2, DEVICE_NETWORK_D0002001 -> 2, DEVICE_ISP_D0003004 -> 2, ZC_益阳市 -> 2, CN_ -> 0, APP_其他 -> 2, DEVICE_OS_D0001001 -> 2, ZP_湘南省 -> 2))
    //useridTagsRDD.foreach(println)


    /*
      (userid, Map(tag, count))
      userid, "app->1, andorid->1"
      USERID:1--->ZC_上海市 -> 2,APP_马上赚 -> 2,LC_02 -> 2,DEVICE_NETWORK_D0002001 -> 2,ZP_上海市 -> 2,DEVICE_ISP_D0003004 -> 2,CN_ -> 0,DEVICE_OS_D0001001 -> 2
    */
    val userid2ContextTagRDD:RDD[(String, String)] = useridTagsRDD.map{case (userid, tags) => {
      (userid, tags.mkString(","))//mkString把一个集合转换为一个字符串
    }}
    //打印
    /*userid2ContextTagRDD.foreach{case (userid, tag) => {
      println(s"${userid}--->${tag}")
    }}*/

    //读取数据库中用户的基础信息为DF
    val url = "jdbc:mysql://localhost:3306/dmp_1807?useUnicode=true&characterEncoding=utf8"
    val properties = new Properties()
    properties.put("user", "root")
    properties.put("password", "274039")
    val userDF = spark.read.jdbc(url, "t_user", properties)
    userDF.createOrReplaceTempView("user_basic_info")

    /**
      * 做用户人口学特征和动态行为信息关联操作
      * 1、将useridTagsRDD转化成为df
      * 2、join操作
      */
    val rowRDD = userid2ContextTagRDD.map{case (userid, tag) => {
      Row(userid, tag)
    }}
    //[USERID:2,LC_02 -> 2,DEVICE_NETWORK_D0002001 -> 2,DEVICE_ISP_D0003004 -> 2,ZC_益阳市 -> 2,CN_ -> 0,APP_其他 -> 2,DEVICE_OS_D0001001 -> 2,ZP_湘南省 -> 2]
    //rowRDD.foreach(println)

    val schema = StructType(List(
      StructField("userid", DataTypes.StringType, false),
      StructField("tag", DataTypes.StringType, false)
    ))
    //将useridTagsRDD转化成为df
    val tagInfoDF:DataFrame = spark.createDataFrame(rowRDD, schema)
    tagInfoDF.createOrReplaceTempView("user_tag_info")

    //打印两个DF
//    tagInfoDF.show()//USERID:2|LC_02 -> 2,DEVICE...
//    userDF.show()// |  2|  孙玉|     1|371323199601274128|17843860331|1998-02-22|  1| 178643@qq.com|     临沂|

    //关联两个DF查询
    val sql =
      """
        |select
        |  t.userid,
        |  b.name,
        |  b.gender,
        |  b.idcard,
        |  b.phone,
        |  b.birthday,
        |  b.cid,
        |  b.email,
        |  b.address,
        |  t.tag
        |from user_tag_info t
        |left join user_basic_info b on substring_index(t.userid, ':', -1) = b.uid
        |
            """.stripMargin
    spark.sql(sql).show()

    /*查询结果：
+--------+----+------+------------------+-----------+----------+---+--------------+-------+--------------------+
|  userid|name|gender|            idcard|      phone|  birthday|cid|         email|address|                 tag|
+--------+----+------+------------------+-----------+----------+---+--------------+-------+--------------------+
|USERID:1|  汤其|     0|371323199601274033|17863860446|1997-01-01|  0|tangqi@163.com|     曲阜|ZC_上海市 -> 2,APP_马...|
|USERID:2|  孙玉|     1|371323199601274128|17843860331|1998-02-22|  1| 178643@qq.com|     临沂|LC_02 -> 2,DEVICE...|
+--------+----+------+------------------+-----------+----------+---+--------------+-------+--------------------+
      */
    spark.stop()
  }

  // 获取用户唯一不为空的ID
  def getNotEmptyID(row: Row): Option[String] = {
    row match {
      case v if v.getAs[String]("userid").nonEmpty => Some("USERID:" + v.getAs[String]("userid").toUpperCase)
      case v if v.getAs[String]("imei").nonEmpty => Some("IMEI:" + v.getAs[String]("imei").replaceAll(":|-\\", "").toUpperCase)
      case v if v.getAs[String]("imeimd5").nonEmpty => Some("IMEIMD5:" + v.getAs[String]("imeimd5").toUpperCase)
      case v if v.getAs[String]("imeisha1").nonEmpty => Some("IMEISHA1:" + v.getAs[String]("imeisha1").toUpperCase)

      case v if v.getAs[String]("androidid").nonEmpty => Some("ANDROIDID:" + v.getAs[String]("androidid").toUpperCase)
      case v if v.getAs[String]("androididmd5").nonEmpty => Some("ANDROIDIDMD5:" + v.getAs[String]("androididmd5").toUpperCase)
      case v if v.getAs[String]("androididsha1").nonEmpty => Some("ANDROIDIDSHA1:" + v.getAs[String]("androididsha1").toUpperCase)

      case v if v.getAs[String]("mac").nonEmpty => Some("MAC:" + v.getAs[String]("mac").replaceAll(":|-", "").toUpperCase)
      case v if v.getAs[String]("macmd5").nonEmpty => Some("MACMD5:" + v.getAs[String]("macmd5").toUpperCase)
      case v if v.getAs[String]("macsha1").nonEmpty => Some("MACSHA1:" + v.getAs[String]("macsha1").toUpperCase)

      case v if v.getAs[String]("idfa").nonEmpty => Some("IDFA:" + v.getAs[String]("idfa").replaceAll(":|-", "").toUpperCase)
      case v if v.getAs[String]("idfamd5").nonEmpty => Some("IDFAMD5:" + v.getAs[String]("idfamd5").toUpperCase)
      case v if v.getAs[String]("idfasha1").nonEmpty => Some("IDFASHA1:" + v.getAs[String]("idfasha1").toUpperCase)

      case v if v.getAs[String]("openudid").nonEmpty => Some("OPENUDID:" + v.getAs[String]("openudid").toUpperCase)
      case v if v.getAs[String]("openudidmd5").nonEmpty => Some("OPENDUIDMD5:" + v.getAs[String]("openudidmd5").toUpperCase)
      case v if v.getAs[String]("openudidsha1").nonEmpty => Some("OPENUDIDSHA1:" + v.getAs[String]("openudidsha1").toUpperCase)
      case _ => None
    }
  }
}

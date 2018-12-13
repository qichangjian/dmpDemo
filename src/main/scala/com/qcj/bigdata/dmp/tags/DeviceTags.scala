package com.qcj.bigdata.dmp.tags
import com.qcj.bigdata.dmp.jobs.constants.TagsConstants
import org.apache.spark.sql.Row

import scala.collection.mutable

/**
  * 获取标签
  * client  设备类型
  * ispname 运营商名称
  * networkmannername 联网方式id
  */
object DeviceTags extends Tags {
  /**
    * 提取对应的标签
    4）设备：操作系统|联网方式|运营商
    设备操作系统
    1	Android	D0001001
    2	IOS	D0001002
    3	Winphone	D0001003
    4	其他	D0001004
    设备联网方式
    WIFI	D0002001
    4G	D0002002
    3G	D0002003
    2G	D0002004
    NWTWORKOTHER	D0004004
    设备运营商方案
    移动	D0003001
    联通	D0003002
    电信	D0003003
    OPERATOROTHER	D0003004
    操作系统：Client
    联网方式：Networkmannerid
    运营商：Ispid
    * @param row
    * @return
    */
  override def extractTag(row: Row): mutable.Map[String, Int] = {
    val map = mutable.Map[String,Int]()
    val client = row.getAs[Int]("client")
    val networkManner = row.getAs[String]("networkmannername")
    val isp = row.getAs[String]("ispname")

    val os = client match {
      case 1 => "D0001001"
      case 2 => "D0001002"
      case 3 => "D0001003"
      case _ => "D0001004"
    }
    map.put(TagsConstants.PRE_TAG_DEVICE_OS + os, 1)

    val network = networkManner.toLowerCase match {
      case "wifi" => "D0002001"
      case "4g" => "D0002002"
      case "3g" => "D0002003"
      case "2g" => "D0002004"
      case _ => "D0004004"
    }
    map.put(TagsConstants.PRE_TAG_DEVICE_NETWORK + network, 1)

    val ispName = isp.toLowerCase() match {
      case "移动" => "D0003001"
      case "联通" => "D0003002"
      case "电信" => "D0003003"
      case _ => "D0003004"
    }

    map.put(TagsConstants.PRE_TAG_DEVICE_ISP + ispName, 1)
    map
  }
}

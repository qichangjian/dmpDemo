package com.qcj.bigdata.dmp.tags

import com.qcj.bigdata.dmp.jobs.constants.TagsConstants
import com.qcj.bigdata.dmp.util.StringUtil
import org.apache.spark.sql.Row
import scala.collection.mutable
/**
  * 地域标签（省标签格式：ZPxxx->1，地市标签格式：ZCxxx->1）xxx为省或市名称
  * 省：Provincename
  * 市：cityname
  */
object ZoneTags extends Tags {
  /**
    * 获取标签
    * provincename: String, //设备所在省份名称
    * cityname: String, //设备所在城市名称
    *
    * @param row
    * @return
    */
  override def extractTag(row: Row): mutable.Map[String, Int] = {
    val provincename = row.getAs[String]("provincename")
    var provincename_count = 0
    val cityname = row.getAs[String]("cityname")
    var cityname_count = 0
    if (!StringUtil.isEmpty(provincename)) {
      provincename_count = 1
    }
    if (!StringUtil.isEmpty(cityname)) {
      cityname_count = 1
    }
    mutable.Map[String, Int](
      TagsConstants.PRE_TAG_ZONE_PROVINCE + provincename -> provincename_count,
      TagsConstants.PRE_TAG_ZONE_CITY + cityname -> cityname_count)
  }
}

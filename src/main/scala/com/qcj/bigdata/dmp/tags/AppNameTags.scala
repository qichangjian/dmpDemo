package com.qcj.bigdata.dmp.tags
import com.qcj.bigdata.dmp.jobs.constants.TagsConstants
import com.qcj.bigdata.dmp.util.StringUtil
import org.apache.spark.sql.Row

import scala.collection.mutable
/**
  * 2）APP名称（标签格式：APPxxxx->1）xxxx为APP的名称，使用缓存文件appname_dict进行名称转换；
    因为对应的appname实际上是当前应用的包名，所以需要apname字典映射表做包名和appname的转换
  */
object AppNameTags extends Tags {
  /**
    * 提取对应的标签
    *
    * @param row
    * @return
    */
  override def extractTag(row: Row): mutable.Map[String, Int] = {
    val appname = row.getAs[String]("appname")
    var count = 0
    if(!StringUtil.isEmpty(appname)){
      count = 1
    }
    mutable.Map[String,Int](TagsConstants.PRE_TAG_APPNAME + appname -> 1)
  }
}

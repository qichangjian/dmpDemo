package com.qcj.bigdata.dmp.tags

import com.qcj.bigdata.dmp.jobs.constants.TagsConstants
import com.qcj.bigdata.dmp.util.StringUtil
import org.apache.spark.sql.Row

import scala.collection.mutable

/**
  * 获取标签
  * keywords: String, //关键字
  * 从当前关键字中可能获得有若干个关键字组成的
  * 5）关键词（标签格式：Kxxx->1）xxx为关键字。关键词个数不能少于3个字符，且不能超过8个字符；关键字中如包含”|”,则分割成数组，转化成多个关键字标签
  “麻辣小龙虾|麻辣香锅|与神对话|家”
	关键字：Keywords
  */
object KeyWordTags extends Tags {
  /**
    * 提取对应的标签
    * 关键词（标签格式：Kxxx->1）xxx为关键字。关键词个长度不能少于3个字符，且不能超过8个字符；关键字中如包含”|”,则分割成数组，转化成多个关键字标签
    “麻辣小龙虾|麻辣香锅|与神对话|家”
	关键字：Keywords
    * @param row
    * @return
    */
  override def extractTag(row: Row) = {
    val keywords = row.getAs[String]("keywords")
    val map =  mutable.Map[String, Int]()
    if(!StringUtil.isEmpty(keywords)) {
      val fields:Array[String] = keywords.split("\\|")

      val filteredKW:Array[String] = fields.filter(kw => kw.length >= 3 && kw.length <= 8)
      filteredKW.foreach(kw => {//拆分为一组循环
        map.put(TagsConstants.PRE_TAG_KEYWORD + kw, 1)
      })
    }
    map
  }
}

package com.qcj.bigdata.dmp.tags

import com.qcj.bigdata.dmp.jobs.constants.TagsConstants
import com.qcj.bigdata.dmp.util.StringUtil
import org.apache.spark.sql.Row

import scala.collection.mutable
/*
3）渠道（标签格式：CNxxxx->1）xxxx为渠道ID
通过频道id确定Channeid:
 */
object ChannelTags extends Tags {
    /**
      * 提取对应的标签
      *
      * @param row
      * @return
      */
    override def extractTag(row: Row) = {
        val channel = row.getAs[String]("channelid")
        var count = 0
        if(!StringUtil.isEmpty(channel)) {
            count = 1
        }
        mutable.Map[String, Int](TagsConstants.PRE_TAG_CHANNEL + channel -> count)
    }
}

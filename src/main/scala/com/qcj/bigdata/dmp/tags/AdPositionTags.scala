package com.qcj.bigdata.dmp.tags

import com.qcj.bigdata.dmp.jobs.constants.TagsConstants
import com.qcj.bigdata.dmp.util.Utils
import org.apache.spark.sql.Row

import scala.collection.mutable

/*
    1）广告位类型（标签格式：LC03->1或者LC16->2）xx为数字，小于10 补0
    ->是权重，这里使用次数做一个说明
    (userid,（01, 1）)
    (userid,（02, 1）)
    (userid,（02, 1）)
 */
object AdPositionTags extends Tags {
    /**
      * 提取对应的标签
      *     Adspacestype:Int	广告位类型（1：banner2：插屏3：全屏）
            Adspacetypename:String	广告位类型名称（banner横幅，插屏，全屏）
      * @param row
      * @return
      */
    override def extractTag(row: Row) = {
        var adspaceType = row.getAs[Int]("adspacetype") + ""
        adspaceType = Utils.fillStr(adspaceType)//调用位数不够前边补0的方法
        //合成对应的标签：LC_ + adspaceType -> 1
        mutable.Map[String, Int](TagsConstants.PRE_TAG_AD_POSTION + adspaceType -> 1)
    }
}

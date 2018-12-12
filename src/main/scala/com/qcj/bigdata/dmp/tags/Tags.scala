package com.qcj.bigdata.dmp.tags

import org.apache.spark.sql.Row

import scala.collection.mutable

/**
  * 标签的父类
  */
trait Tags {
    /**
      * 提取对应的标签
      * @param row
      * @return
      */
    def extractTag(row:Row):mutable.Map[String, Int]
}

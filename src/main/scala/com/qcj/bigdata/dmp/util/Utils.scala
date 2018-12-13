package com.qcj.bigdata.dmp.util

import scala.collection.mutable

/**
  * 日期时间处理工具
  * 字符串处理工具
  */
object Utils {

  //循环合并到一个新map中
  def addTags(tags: mutable.Map[String, Int]*): mutable.Map[String, Int] = {
    var newTags = mutable.Map[String, Int]()
    tags.foreach( tag =>
      newTags = newTags.++(tag)
    )
    newTags
  }


  //2016-10-01 06:19:17
    def fmtHour(str: String) = {
        if(StringUtil.isEmpty(str)) {
            None
        } else {
            val hour = str.substring(str.indexOf(" ") + 1, str.indexOf(" ") + 3)
            if(StringUtil.isEmpty(hour)) {
                None
            } else {
                Option(hour)
            }
        }
    }


    //2016-10-01 06:19:17
    def fmtDate(str: String):Option[String] = {
        if(StringUtil.isEmpty(str)) {
            None
        } else {
            val date = str.substring(0, str.indexOf(" "))
            if(StringUtil.isEmpty(date)) {
                None
            } else {
                Option(date)
            }
        }
    }

    def parseDouble(str: String): Double = {
        if(StringUtil.isEmpty(str)) {
            0.0
        } else {
            try {
//                java.lang.Double.valueOf(str)
                str.toDouble
            } catch {
                case ne:NumberFormatException => {
                    0.0
                }
                // switch(xx) case中的default
                case _ => throw new RuntimeException("异常数字没有被捕获到")
            }

        }
    }

    def parseInt(str: String): Int = {
        if(StringUtil.isEmpty(str)) {
            0
        } else {
            try {
                str.toInt
            } catch {
                case ne:NumberFormatException => {
                    0
                }
                // switch(xx) case中的default
                case _ => throw new RuntimeException("异常数字没有被捕获到")
            }

        }
    }

  /**
    * 位数不够前边补0的方法
    */
    def fillStr(str:String):String = {
        if(str.length < 2) {
            "0" + str
        } else {
            str
        }
    }
    def main(args: Array[String]): Unit = {
        println(fmtDate("2016-10-01 06:19:17"))
        println(fmtHour("2016-10-01 06:19:17"))
    }
}

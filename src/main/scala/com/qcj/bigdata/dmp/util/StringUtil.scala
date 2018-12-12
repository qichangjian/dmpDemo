package com.qcj.bigdata.dmp.util

import scala.collection.mutable

object StringUtil {
    /**
      * A=100|C=99
      * @param str
      * @param separator
      * @param field
      * @param value
      */
    def setStrField(str:String, separator:String, field:String, value:Int): String = {
        if(isEmpty(str) || isEmpty(field)) {
            str
        } else {
            val kvs = str.split(separator)
            if(!kvs.isEmpty) {
                val pairs = new mutable.HashMap[String, Int]()
                kvs.foreach(kv => {
                    val pair = kv.split(" -> ")
                    pairs.put(pair(0), pair(1).toInt)
                })
                pairs.put(field, pairs.getOrElse(field, 0) + value)
                pairs.mkString("|")
            } else {
                field + " -> " + value
            }
        }
    }

    /**
      *
      * @param str
      * @param separator
      * @param field
      * @return
      */
    def getStrField(str:String, separator:String, field:String):String = {
        if(isEmpty(str) || isEmpty(field)) {
            null
        } else {
            val kvs = str.split(separator)
            val fieldMap:Map[String, String] = kvs.map(kv => {
                val pair = kv.split(" -> ")
                (pair(0), pair(1))
            }).toMap
            fieldMap.getOrElse(field, "")
        }
    }

    def isEmpty(str:String):Boolean = {
        (str == null) || str.isEmpty
    }

    def main(args: Array[String]): Unit = {
        var str = "A -> 100|C -> 99"
        println(getStrField(str, "\\|", "C"))
        str = setStrField(str, "\\|", "B", 12)
        println(str)
        println(setStrField(str, "\\|", "C", 1))
    }
}

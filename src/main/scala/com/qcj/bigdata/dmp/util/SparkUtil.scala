package com.qcj.bigdata.dmp.util

import org.apache.spark.{SparkConf, SparkContext}

object SparkUtil {

    def sparkContext(master:String, appName:String): SparkContext = {
        val conf = sparkConf(master, appName)
        sparkContext(conf)
    }

    def sparkContext(conf:SparkConf): SparkContext = {
        new SparkContext(conf)
    }

    def sparkConf(master:String, appName:String):SparkConf = {
        val conf = new SparkConf()
        conf.setAppName(appName)
        conf.setMaster(master)
        conf
    }
}

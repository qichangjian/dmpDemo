package com.qcj.bigdata.dmp.jobs.report

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 终端设备：
  *   client设备类型
  */
object ReportByTerminalAndClient {
  def main(args: Array[String]): Unit = {
    /*if(args == null || args.length < 2) {
      println(
        """Parameter Errors! Usage: <inputpath> <table>
          |inputpath    :   输入目录
          |table        :   输出表名
        """.stripMargin)
      System.exit(-1)
    }
    val Array(inputpath, table) = args*/

    val conf  = new SparkConf()
      .setAppName("ReportByTerminalAndClient")
      .setMaster("local[2]")

    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    val df = spark.read.parquet("data/out/")
    df.createOrReplaceTempView("ad_logs_tmp")//创建视图

    val sql =
      """
        |select
        |   date_add(current_date(),-1) `date`,
        |   client,
        |   count(case
        |            when requestmode = 1 and processnode >= 1
        |             then 1
        |      end ) request_num_total,
        |   count(case
        |            when requestmode = 1 and processnode >= 2
        |             then 1
        |      end ) request_num_valid,
        |   count(case
        |            when requestmode = 1 and processnode = 3
        |              then 1
        |      end ) request_num_ad,
        |   count(case
        |            when adplatformproviderid >= 100000 and iseffective = 1 and isbilling = 1 and isbid = 1 and adorderid != 0
        |             then 1
        |      end ) bid_num_participate,
        |   count(case
        |            when adplatformproviderid >= 100000 and iseffective = 1 and isbilling = 1 and iswin = 1
        |             then 1
        |      end ) bid_num_success,
        |   count(case
        |            when requestmode = 2 and iseffective = 1 and isbilling = 1
        |             then 1
        |      end ) ad_num_show,
        |   count(case
        |            when requestmode = 3 and iseffective = 1 and isbilling = 1
        |             then 1
        |      end ) ad_num_click,
        |   sum(case
        |            when adplatformproviderid >= 100000 and iseffective = 1 and isbilling = 1 and iswin = 1 and adorderid >= 200000 and adcreativeid >= 200000
        |             then winprice
        |             else 0
        |      end ) / 1000 ad_cost,
        |   sum(case
        |            when adplatformproviderid >= 100000 and iseffective = 1 and isbilling = 1 and iswin = 1 and adorderid >= 200000 and adcreativeid >= 200000
        |            then adpayment
        |            else 0
        |      end ) / 1000 ad_payment
        |from ad_logs_tmp
        |group by client
      """.stripMargin

    val retDF = spark.sql(sql)
    retDF.show()

    //结果入库
    val url = "jdbc:mysql://localhost:3306/dmp_1807"
    val properties = new Properties()
    properties.put("user","root")
    properties.put("password","274039")
    retDF.write.mode(SaveMode.Append).jdbc(url,"r_1807_client",properties)
    spark.stop()
  }
}

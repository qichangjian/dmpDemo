package com.qcj.bigdata.dmp.jobs.report

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 基于省市地域的报表统计
  *
  * 省市/城市	    总请求	有效请求	广告请求	参与竞价数	竞价成功数 	竞价成功率	展示量	点击量	点击率	广告成本	广告消费
  * -A省
  * B市
  **
  *统计每天的数据，将最终结果存储到MySQL
  **
  *mysql表结构
  *r_1807_area
  *接下来的统计，都是记录标准目录下面的数据
  *hdfs://ns1/standard/ad/2018/12/11/ad_access.log.2018-12-11.parquet
  *
  *
  * 注意在sql中使用count和sum的区别
  *     count是统计行，sum是统计的列对应的值
  */
 /* 项目运行步骤：
    1.准备工作：创建数据库和创建表
      scripts下的dmp-1807-create.sql
    2.运行项目
    参数设置：data/out/  r_1807_area
    上一个项目etl出来的parquet结果查询 整理 然后 输出到mysql中
  *
  */
object ReportByProvinceAndCity {
  def main(args: Array[String]): Unit = {
    if(args == null || args.length < 2) {
      println(
        """Parameter Errors! Usage: <inputpath> <table>
          |inputpath    :   输入目录
          |table        :   输出表名
        """.stripMargin)
      System.exit(-1)
    }
    val Array(inputpath, table) = args

    val conf = new SparkConf()
      .setAppName("ReportByProvinceAndCity")
      .setMaster("local[2]")

    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    //读取标准目录下面的数据
   /* val df = spark.read.parquet("data/out")
    df.show()//测试连接*/
    val df = spark.read.parquet(inputpath)
    df.createOrReplaceTempView("ad_logs_tmp")//创建视图


    //使用sparksql进行统计结果:统计前一天
    val sql =
      """
        |select
        |   date_add(current_date(), -1) `date`,
        |   provincename as province,
        |   cityname as city,
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
        |group by provincename, cityname
      """.stripMargin

    val retDF = spark.sql(sql)
    //        retDF.show()
    //结果入库
    val url = "jdbc:mysql://localhost:3306/dmp_1807"
    val properties = new Properties()
    properties.put("user","root")
    properties.put("password","274039")
    retDF.write.mode(SaveMode.Append).jdbc(url,table,properties)
    spark.stop()
  }
}

package com.qcj.bigdata.dmp.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}

//获得hbase的HBaseConnection对象
object HBaseConnectionUtil {

    def getConnection(): Connection = {
        val conf:Configuration = HBaseConfiguration.create()
      //拷贝配置文件过来就不用这两个set
        conf.set("hbase.rootdir", "hdfs://bd1807/hbase")
        conf.set("hbase.zookeeper.quorum", "hadoop1,hadoop2,hadoop3")
        val connection = ConnectionFactory.createConnection(conf)
        connection
    }

    //test client hbase数据库
    def main(args: Array[String]): Unit = {
        val connection = getConnection()

        val admin = connection.getAdmin

        val tableNames = admin.listTableNames()//返回hbase表数组集合
        println(tableNames.mkString("[", ", ", "]"))
    }
}

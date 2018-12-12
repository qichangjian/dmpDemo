package com.qcj.bigdata.dmp.jobs.etl

import com.qcj.bigdata.dmp.entity.Logs
import org.apache.spark.SparkConf
import org.apache.spark.io.SnappyCompressionCodec
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 需求一：数据的清洗ETL
  * 1）要求一：将数据转换成parquet文件格式
  * 2）要求二：序列化方式采用KryoSerializer方式
  * 3）要求三：parquet文件采用Sanppy压缩方式
  **               parquet需要sparkSql  KryoSerializer需要实现实体
  * 一：
  *1.原始数据：
  *这数据会存储在hdfs里面，假如
  *hdfs://bd1807/orginal/ad/2018/12/ad_access.log.2018-12-10
  *hdfs://bd1807/orginal/ad/2018/12/ad_access.log.2018-12-11
  **
  *2.需要对上述目录下每天产生的文件进行格式转化，转化为以Snappy压缩，采用KryoSerializer序列化的方式的额parquet文件
  *转化完毕，即可对保留七天的数据进行删除。
  *转化目的地：
  *hdfs://bd1807/standard/ad/2018/12/10/ad_access.log.2018-12-10.parquet
  *hdfs://bd1807/standard/ad/2018/12/11/ad_access.log.2018-12-11.parquet
  **
  *3.后期所有的统计操作，基于standard目录下面的数据进行操作。
  **
  *二：这么做的好处：
  *1、节省了存储空间
  *2、提高计算效率
  *3、数据便的相对安全
  *4、方便使用hive和SparkSQL进行统计分析
  *
  *三：aused by: org.apache.parquet.schema.InvalidSchemaException: Cannot write a schema with an empty group: message spark_schema
  * 在使用Scala class做rdd->df中的反射字节码对象的时候，InvalidSchema也就是说并没有从自定义的class中提取到对应的字段
  * 解决：
  *   所以在实例类中继承实现Product
  */
/* 运行过程
  *先在本地运行：
  *   1.设置运行参数：data/data.txt data/out/ 运行 结果在data/out下 local[2]所以有两个文件
  *
  *在集群上运转
  *   1.准备工作
  *   hdfs上创建数据文件存储目录并上传文件
  *   hdfs dfs -mkdir -p hdfs://bd1807/orginal/ad/2018/12/
  *   hdfs dfs -put data.txt hdfs://bd1807/orginal/ad/2018/12/ad_access.log.2018-12-10
  *   2.打包项目（需要用到第三方依赖工具类，要完整打包）
  *     a.需要用到配置文件，将bat放开
  *     b.在终端（Terminal）打包:(跳过测试)
  *      mvn clean package -DskipTests
  *     c.上传linux
  *     com.qcj.bigdata.dmp.jobs.etl.File2ParquetJob
  *     d.编写submit脚本
        vim spark-submit-wc.sh
#!/bin/sh
YEAR=`date -d "1 day ago" +%Y`
MONTH=`date -d "1 day ago" +%m`
DAY=`date -d "1 day ago" +%d`

#echo "hdfs://bd1807/orginal/ad/${YEAR}/${MONTH}/ad_access.log.${YEAR}-${MONTH}-${DAY}"

export HADOOP_CONF_DIR=/home/hadoop1/apps/hadoop-2.7.6/etc/hadoop
/home/hadoop1/apps/spark-2.2.2-bin-hadoop2.7/bin/spark-submit \
--class  com.qcj.bigdata.dmp.jobs.etl.File2ParquetJob \
--executor-memory 600M \
--driver-memory 600M \
--num-executors 1 \
--total-executor-cores 1 \
/home/hadoop1/project/jar/dmp1087/dmp-1087-1.0-SNAPSHOT-jar-with-dependencies.jar \
hdfs://bd1807/orginal/ad/${YEAR}/${MONTH}/ad_access.log.${YEAR}-${MONTH}-${DAY} \
hdfs://bd1807/standard/ad/${YEAR}/${MONTH}/${DAY}

  *   3.定时调度
  *   注意：别忘了删除hdfs上的目录：
  *   hdfs dfs -R /standard/ad/2018/12
  *    crontab -e
  *   08 21 * * * sh /home/hadoop1/project/sh/spark-submit-wc.sh >> /home/hadoop1/project/sh/elt.log
  *   什么时间执行submit.sh 打印日志到什么位置
  */

object File2ParquetJob {
  def main(args: Array[String]): Unit = {

    if(args == null || args.length < 2){
      println(
        """Parameter Errors!Usage:<inputPath><outputPath>
          |inputPath            :  输入文件路径
          |outputPath           :  程序输入文件路径
        """.stripMargin)
      System.exit(0)
    }
    val Array(inputPath,outputPath) = args

    val conf = new SparkConf()
      .setAppName("File2ParquetJob").setMaster("local[2]")
      //使用kryo的序列化方式，并注册
      .set("spark.serializer",classOf[KryoSerializer].getName)
      .registerKryoClasses(Array(classOf[Logs]))
      //指定snappy压缩算法
      .set("spark.io.compression.codec", classOf[SnappyCompressionCodec].getName)

    val spark = SparkSession.builder().config(conf).getOrCreate()
    //BeanClass方式自动会有Schema信息 --->Schema信息
    val sqlContext = spark.sqlContext

    //第一步加载的原始数据
    val linesRDD = spark.sparkContext.textFile(inputPath)//"data/data.txt"

    /*
        1、不是parquet格式的文件，而且在这里也没有办法使用KryoSerializer
        2、通过sqlContext创建的DataFrame，两种方式：
            1）、要么使用反射的方式
            2）、要么使用动态编程
            在这使用动态编程的无法完成高性能的压缩，所以就使用第一种反射的方式
     */
    val logRDD:RDD[Logs] = linesRDD.map(line=> Logs.line2Logs(line))

    // rdd->df中的反射字节码对象
    import sqlContext.implicits._//导入sqlContext中的隐式转换
    val logsDF = logRDD.toDF()//转换为DF
    //parquet的方式写出去
    logsDF.write.mode(SaveMode.Overwrite).parquet(outputPath)//mode文件目录存在是覆盖:输出路径

    spark.stop()
  }
}

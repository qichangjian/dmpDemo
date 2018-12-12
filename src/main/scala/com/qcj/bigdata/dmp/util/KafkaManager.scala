package com.qcj.bigdata.dmp.util

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.curator.framework.CuratorFramework
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{KafkaUtils, OffsetRange}

object KafkaManager {

    /**
      *
      * @param ssc
      * @param kafkaParams
      * @param topic
      * @param group
      * @param client CuratorFramework实例
      * @param zkTopicOffsetPath 该偏移量在zk中的根路径
      * @return
      */
    def createMessage(ssc: StreamingContext, kafkaParams: Map[String, String], topic: String, group:String, client: CuratorFramework, zkTopicOffsetPath:String): InputDStream[(String,String)] = {
        //3、获取topic对应的offsets信息
        val (fromOffsets, flag) = getFromOffsets(client, topic, group, zkTopicOffsetPath)
        //2、搭建基础架构
        var message:InputDStream[(String, String)] = null
        if(flag) {//标记使用从zk中得到了对应partition偏移量信息，如果有为true
        val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.message)
                message = KafkaUtils.createDirectStream[
                    String,
                    String,
                    StringDecoder,
                    StringDecoder,
                    (String,String)](
                    ssc, kafkaParams,
                    fromOffsets, messageHandler//该fromOffsets必须要自己从zk中读取
                )
        } else {//没有得到，为false
            message = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topic.split(",").toSet)
        }
        message
    }

    /**
      * 获取对应topic中的每一个partition的偏移量信息
      */
    def getFromOffsets(client:CuratorFramework, topic:String, group:String, zkTopicOffsetPath:String):(Map[TopicAndPartition, Long], Boolean) = {

        val zkPath = s"${zkTopicOffsetPath}/${topic}/${group}"
        //读取offset信息
        ensureZKPathExists(client, zkPath)

        //读取该目录下面的所有的子节点信息
        import scala.collection.JavaConversions._
        val offsets = for{ p <- client.getChildren.forPath(zkPath)} yield {
            val offset = new String(client.getData.forPath(s"${zkPath}/${p}")).toLong
            (TopicAndPartition(topic, p.toInt), offset)
        }

        if(offsets.isEmpty) {//无偏移量信息
            (offsets.toMap, false)
        } else {//有偏移量信息
            (offsets.toMap, true)
        }
    }

    def ensureZKPathExists(client:CuratorFramework, path:String): Unit = {
        if(client.checkExists().forPath(path) == null) {//确保读取的目录一定是存在的
            //创建之
            client.create().creatingParentsIfNeeded().forPath(path)
        }
    }


    /*
        将最新的offset信息更新回去
        OffsetRange就代表了读取到的偏移量的数据范围
     */
    def storeOffsets(offsetRanges:Array[OffsetRange], group:String, zkTopicOffsetPath:String, client:CuratorFramework): Unit = {
        for(offsetRange <- offsetRanges) {
            val partition = offsetRange.partition
            val topic = offsetRange.topic
            val offset = offsetRange.untilOffset
            val path = s"${zkTopicOffsetPath}/${topic}/${group}/${partition}"
            ensureZKPathExists(client, path)
            client.setData().forPath(path, (offset + "").getBytes())
        }
    }
}

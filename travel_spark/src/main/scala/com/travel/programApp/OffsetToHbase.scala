package com.travel.programApp

import java.util.regex.Pattern

import com.travel.common.{ConfigUtil, Constants, HBaseUtil, JedisUtil}
import com.travel.utils.HbaseTools
import com.travel.utils.HbaseTools.getHbaseConn
import org.apache.hadoop.hbase.{Cell, CellUtil, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, Get, Put, Result, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, ConsumerStrategy, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable

/**
 * 实现 消费 kafka 数据  手动维护 offset
 */
object OffsetToHbase {
  def main(args: Array[String]): Unit = {

    //创建sparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("offsetToHase").setMaster("local[2]")
    //创建sparksession
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    //获取sparkContext
    val sparkContext: SparkContext = sparkSession.sparkContext
    //创建streamingContext
    val streamingContext = new StreamingContext(sparkContext, Seconds(2))

    /**
     * 查询Hbase 中是否 已经消费  已经有分区的offset值
     * 设计到 Hbase 表的设计   表名
     * 列族 f1 ：  存在多个列  消费者组：主题 ：分区
     * group1：topic：partition1     group1：topic：partition1   group2：topic：partition2     group2：topic：partition2
     * rowkey：消费者组：主题
     * group1：topic         long 25535                  long 25535
     * group2：topic                                                               long 25535                        long 25535F
     */

    /**
     *
     * SubscribePattern 通过正则匹配订阅主题   如果Hase 中 没有数据 调用两个参数的方法  有数据 调用三个参数的方法
     *
     * pattern : java.util.regex.Pattern,
     * kafkaParams : java.util.Map[scala.Predef.String,
     * java.lang.Object  =>  Map[TopicPartition, Long]
     *
     * pattern : java.util.regex.Pattern,
     * * kafkaParams : java.util.Map[scala.Predef.String,
     *
     */

    //定义订阅主题使用的正则 pattern

    val regex: String = "(.*)gps_topic"

    val pattern: Pattern = Pattern.compile(regex)

    //kafkaParams
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "node01:9092,node02:9092,node03:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "gps_consum_group",
      "auto.offset.reset" -> "latest", // earliest,latest,和none
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    //从Hase中获取每个分区的offset  返回  Map[TopicPartition, Long]

    //获取Hbase 的链接
    val hbaseConn: Connection = getHbaseConn

    val admin: Admin = hbaseConn.getAdmin

    //判断Hbase表是否存在
    if (!admin.tableExists(TableName.valueOf("hbase_offset_store"))) {

      //表不存在创建表
      val hbase_offset_store = new HTableDescriptor(TableName.valueOf("hbase_offset_store"))

      hbase_offset_store.addFamily(new HColumnDescriptor("f1"))

      admin.createTable(hbase_offset_store);

      admin.close()
    }

    // 连接表
    val table: Table = hbaseConn.getTable(TableName.valueOf(ConfigUtil.getConfig("hbase_offset_store")))

    var topicPartitionToLong: collection.Map[TopicPartition, Long] = new mutable.HashMap[TopicPartition, Long]()

    //获取每个主题的分区offset值

    val topics = Array("cheng_du_gps_topic", "hai_kou_gps_topic")

    for (eachTopic <- topics) {

      val get = new Get(("gps_consum_group" + ":" + eachTopic).getBytes())

      val result: Result = table.get(get)

      val cells: Array[Cell] = result.rawCells()

      for (cell <- cells) {

        //获取列名  group:topic:partition
        val column: String = Bytes.toString(CellUtil.cloneQualifier(cell))

        val splitName: Array[String] = column.split(":")

        val topic: String = splitName(1)

        val partiton: String = splitName(2)

        val topicPartition = new TopicPartition(topic, partiton.toInt)

        val offset_value: Long = Bytes.toLong(CellUtil.cloneValue(cell))

        topicPartitionToLong += (topicPartition -> offset_value.toLong)

      }
    }

    table.close()

    //判断 map 中是否有值
    val result = if (topicPartitionToLong.size > 0) {
      //证明已经消费了
      val consumerStrategy: ConsumerStrategy[String, String] = ConsumerStrategies.SubscribePattern[String, String](pattern, kafkaParams, topicPartitionToLong)
      val value: InputDStream[ConsumerRecord[String, String]] = KafkaUtils
        .createDirectStream(streamingContext, LocationStrategies.PreferConsistent, consumerStrategy)
      value
    } else {

      val consumerStrategy: ConsumerStrategy[String, String] = ConsumerStrategies.SubscribePattern[String, String](pattern, kafkaParams)

      val value: InputDStream[ConsumerRecord[String, String]] = KafkaUtils
        .createDirectStream(streamingContext, LocationStrategies.PreferConsistent, consumerStrategy)
      value
    }

    /**
     * 更新offset 的值 到hbase
     */

    result.foreachRDD(eachRdd => {
      if (!eachRdd.isEmpty()) {
        eachRdd.foreachPartition(eachPartition => {
          val connection: Connection = HBaseUtil.getConnection
          val jedis: Jedis = JedisUtil.getJedis
          //判断表是否存在，如果不存在就进行创建
          val admin: Admin = connection.getAdmin
          if (!admin.tableExists(TableName.valueOf(Constants.HTAB_GPS))) {
            val htabgps = new HTableDescriptor(TableName.valueOf(Constants.HTAB_GPS))
            htabgps.addFamily(new HColumnDescriptor(Constants.DEFAULT_FAMILY))
            admin.createTable(htabgps)
          }
          if (!admin.tableExists(TableName.valueOf(Constants.HTAB_HAIKOU_ORDER))) {
            val htabgps = new HTableDescriptor(TableName.valueOf(Constants.HTAB_HAIKOU_ORDER))
            htabgps.addFamily(new HColumnDescriptor(Constants.DEFAULT_FAMILY))
            admin.createTable(htabgps)
          }
          eachPartition.foreach(record => {
            //保存到HBase和redis
            val consumerRecords: ConsumerRecord[String, String] = HbaseTools.saveToHBaseAndRedis(connection, jedis, record)
          })
          JedisUtil.returnJedis(jedis)
          connection.close()
        })

        val offsetRanges: Array[OffsetRange] = eachRdd.asInstanceOf[HasOffsetRanges].offsetRanges
        for (offserRange <- offsetRanges) {
          val endOffset: Long = offserRange.untilOffset //结束offset
          val topic: String = offserRange.topic
          val partition: Int = offserRange.partition
          val conn: Connection = HbaseTools.getHbaseConn
          val table: Table = conn.getTable(TableName.valueOf("hbase_offset_store"))
          val rowkey = "gps_consum_group" + ":" + topic
          val columName = "gps_consum_group" + ":" + topic + ":" + partition
          val put = new Put(rowkey.getBytes())
          put.addColumn("f1".getBytes(), columName.getBytes(), endOffset.toString.getBytes())
          table.put(put)
          table.close()
          conn.close()

        }
      }
    }
    )
    streamingContext.start()

    streamingContext.awaitTermination()
  }
}

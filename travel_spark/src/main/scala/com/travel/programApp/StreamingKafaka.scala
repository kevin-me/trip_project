package com.travel.programApp

import java.util.regex.Pattern

import com.travel.common.{ConfigUtil, Constants, HBaseUtil, JedisUtil}
import com.travel.utils.HbaseTools
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import redis.clients.jedis.Jedis

import scala.collection.mutable

/**
 * 使用sparkStreaming去消费kafka的数据，然后自主维护offset值到hbase里面去
 *
 */
object StreamingKafaka {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("streamingKafka")

    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val context = sparkSession.sparkContext
    context.setLogLevel("WARN")


    val streamingContext = new StreamingContext(context, Seconds(1))



    /**
     * ssc: StreamingContext,
     * locationStrategy: LocationStrategy,
     * consumerStrategy: ConsumerStrategy[K, V]
     */



    val brokers = ConfigUtil.getConfig(Constants.KAFKA_BOOTSTRAP_SERVERS)
    //定义topic
    val topics = Array(ConfigUtil.getConfig(Constants.CHENG_DU_GPS_TOPIC),ConfigUtil.getConfig(Constants.HAI_KOU_GPS_TOPIC))
    val conf = new SparkConf().setMaster("local[1]").setAppName("sparkKafka")
    val group:String = "gps_consum_group"
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "auto.offset.reset" -> "latest",// earliest,latest,和none
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )


    /**
     * 查询hbase的offset
     * 如何设计hbase的表结构，来实现offset的保存
     * group   topic     partition    ===>  offset  值
     */
   /* val connection = HbaseTools.getHbaseConn
    val admin = connection.getAdmin
    if(!admin.tableExists(TableName.valueOf(Constants.HBASE_OFFSET_STORE_TABLE))){
      val htableDescriptor = new HTableDescriptor(TableName.valueOf(Constants.HBASE_OFFSET_STORE_TABLE))
      htableDescriptor.addFamily(new HColumnDescriptor(Constants.HBASE_OFFSET_FAMILY_NAME))
      admin.createTable(htableDescriptor)
      admin.close()
    }

    val table = connection.getTable(TableName.valueOf(ConfigUtil.getConfig(Constants.HBASE_OFFSET_STORE_TABLE)))
    val myReturnValue = new mutable.HashMap[TopicPartition, Long]()

    //根据消费者组，以及topic的名字，来获取每个topic所有的offset的值
    for(eachTopic <- topics){
      val rowkey = group+":" +  eachTopic
      val get = new Get(rowkey.getBytes())

      val result = table.get(get)
      val cells = result.rawCells()  //获取hbase的每一个cell值
      for(eachCell <- cells){
        ///得到了列名
        val topicPartiton = Bytes.toString(CellUtil.cloneQualifier(eachCell))
        //获取列值  offset的值
        val offsetValue = Bytes.toString(CellUtil.cloneValue(eachCell))
        val strings = topicPartiton.split(":")
        val topic  = strings(1)
        val partitions = strings(2)
        val partition = new TopicPartition(topic, partitions.toInt)
        myReturnValue  += (partition -> offsetValue.toLong)
      }
    }



   val myResultStragety =  if(myReturnValue.size > 0){
      //证明hbase表当中已经有了offset的值
      ConsumerStrategies.SubscribePattern(Pattern.compile("(.*)gps_topic"),kafkaParams,myReturnValue)
    }else{
      //hbase当中还没有offset的值
      ConsumerStrategies.SubscribePattern(Pattern.compile("(.*)gps_topic"),kafkaParams)
    }

    //判断hbase当中是否有保存offset的表，如果没有，创建，然后查询hbase的offset，如果有表，直接查询hbase的offset

    //消费kafak的数据，将offset的值保存到了hbase里面去了
    val inputDStream = KafkaUtils.createDirectStream(streamingContext, LocationStrategies.PreferConsistent, myResultStragety)
*/
   val result: InputDStream[ConsumerRecord[String, String]] = HbaseTools.getStreamingContextFromHBase(streamingContext, kafkaParams, topics, group, "(.*)gps_topic")

    result.foreachRDD(eachRDD =>{
      if(!eachRDD.isEmpty()){

        eachRDD.foreachPartition(eachPartition =>{
          //将数据保存到hbase以及redis里面去
          val conn = HbaseTools.getHbaseConn
         // val connection = HBaseUtil.getConnection
          val jedis = JedisUtil.getJedis

          eachPartition.foreach(record =>{
            //将每一条数据保存到hbase以及redis里面去
            HbaseTools.saveToHBaseAndRedis(conn,jedis,record)
          })

          JedisUtil.returnJedis(jedis)
          conn.close()
        })

        //提交offset的值，将offset保存到hbase里面去
        val offsetRanges = eachRDD.asInstanceOf[HasOffsetRanges].offsetRanges

        for(eachRange <- offsetRanges){
          val startoffset = eachRange.fromOffset
          val endOffset = eachRange.untilOffset
          val topic = eachRange.topic
          val partition = eachRange.partition
          HbaseTools.saveBatchOffset(group,topic,partition+"",endOffset)
        }
      }

    })

    streamingContext.start()
    streamingContext.awaitTermination()


  }


}

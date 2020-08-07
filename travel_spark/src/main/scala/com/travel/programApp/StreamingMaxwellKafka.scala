package com.travel.programApp

import com.travel.common.{ConfigUtil, Constants}
import com.travel.utils.{HbaseTools, JsonParse}
import org.apache.hadoop.hbase.client.Connection
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingMaxwellKafka {

  def main(args: Array[String]): Unit = {
    val brokers = ConfigUtil.getConfig(Constants.KAFKA_BOOTSTRAP_SERVERS)
    val topics = Array(Constants.VECHE)
    val conf = new SparkConf().setMaster("local[4]").setAppName("sparkMaxwell")
    val group_id: String = "vech_group"
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group_id,
      "auto.offset.reset" -> "earliest", // earliest,latest,和none
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val context: SparkContext = sparkSession.sparkContext
    context.setLogLevel("WARN")
    // val streamingContext = new StreamingContext(conf,Seconds(5))
    //获取streamingContext
    val ssc: StreamingContext = new StreamingContext(context, Seconds(1))


    val getDataFromKafka: InputDStream[ConsumerRecord[String, String]] = HbaseTools.getStreamingContextFromHBase(ssc, kafkaParams, topics, group_id, "veche")

    //处理数据，将数据保存到hbase里面去
    getDataFromKafka.foreachRDD(eachRdd => {

      if (!eachRdd.isEmpty()) {
        eachRdd.foreachPartition(eachPartition => {
          val conn: Connection = HbaseTools.getHbaseConn

          eachPartition.foreach(eachLine => {
            //通过value获取到了一行字符串  ，{"database":"test","table":"myuser","type":"insert","ts":1596631243,"xid":714,"commit":true,"data":{"id":121,"name":"xxx","age":null}}
            val jsonStr: String = eachLine.value()
            val parse: (String, Any) = JsonParse.parse(jsonStr)
            HbaseTools.saveBusinessDatas(parse._1, parse, conn)

          })
          //关闭连接
          HbaseTools.closeConn(conn)
        })
        //提交每一个rdd的offset的值
        val offsetRanges: Array[OffsetRange] = eachRdd.asInstanceOf[HasOffsetRanges].offsetRanges
        //将offset提交到了kafka的一个topic里面去保存了
        //  getDataFromKafka.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
        for (eachrange <- offsetRanges) {
          val startoffset: Long = eachrange.fromOffset
          val endOffset: Long = eachrange.untilOffset
          val topic: String = eachrange.topic
          val partition: Int = eachrange.partition
          HbaseTools.saveBatchOffset(group_id, topic, partition + "", endOffset)

        }
      }
    })
    ssc.start()
    ssc.awaitTermination()

  }

}

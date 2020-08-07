//package com.travel.programApp.seek
//
//object seekzook {
//
//  private def readOffset(topics:Seq[String] , groupId:String):Map[TopicPartition , Long] = {
//    val topicPartitionMap = collection.mutable.HashMap.empty[TopicPartition , Long]
//
//    //拿topic和分区信息
//    val topicAndPartitionMaps: mutable.Map[String, Seq[Int]] = zkUtils.getPartitionsForTopics(topics)
//
//    topicAndPartitionMaps.foreach(topicPartitions =>{
//      val zkGroupTopicsDirs: ZKGroupTopicDirs = new ZKGroupTopicDirs(groupId , topicPartitions._1)
//      topicPartitions._2.foreach(partition =>{//迭代分区
//        val offsetPath = s"${zkGroupTopicsDirs.consumerOffsetDir}/${partition}"
//        val  tryGetTopicPartition = Try{
//          //String --->offset
//          val offsetTuples: (String, Stat) = zkUtils.readData(offsetPath)
//          if(offsetTuples != null){
//            topicPartitionMap.put(new TopicPartition(topicPartitions._1 , Integer.valueOf(partition)) , offsetTuples._1.toLong)
//          }
//        }
//        if(tryGetTopicPartition.isFailure){
//          val consumer = new KafkaConsumer[String , Object](kafkaParams)
//          val topicCollection = List(new TopicPartition(topicPartitions._1 , partition))
//          consumer.assign(topicCollection)
//
//          val avaliableOffset: Long = consumer.beginningOffsets(topicCollection).values().head
//
//          consumer.close()
//          topicPartitionMap.put(new TopicPartition(topicPartitions._1 , Integer.valueOf(partition)) , avaliableOffset)
//
//        }
//      })
//    })
//
//    //currentoffset 、 earliestoffset  leatestOffset
//    //cur < ear || cur > leaty ==> 矫正-->  ear
//    //TODO 矫正
//    val earliestOffsets = getEarliestOffsets(kafkaParams , topics)
//
//
//    val latestOffsets = getLatestOffsets(kafkaParams , topics)
//    for((k,v) <- topicPartitionMap){
//      val current = v
//      val earliest = earliestOffsets.get(k).get
//      val latest = latestOffsets.get(k).get
//      if(current < earliest || current > latest){
//        topicPartitionMap.put(k , earliest)
//      }
//    }
//    topicPartitionMap.toMap
//  }
//
//   getEarliestOffsets(kafkaParams,topics)
//
//}

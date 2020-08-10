# trip_project
简述本项目当中处理的整个流程以及使用到了哪些框架技术点
出行项目主要有两个数据处理流程的分支
 1.业务库数据 通过maxwell 实时解析 mysql 的binlog 把数据发送到kafka 然后 sparkStreaming 消费kafka数据 存入Hbase
 2.日志数据 通过 flume 实时采集 到kafka 然后由sparkStreaming 消费 数据保存到redis 和 Hbase 
     A.kafka的offset手动维护到Hbase中
     B.phoenix映射Hbase当中的表
     C.虚拟车站的统计 涉及到 把 每个区的边界数据 广播出去 和 udf 自定义函数 计算 h3 算法的值
(1)从日志数据乘集开始描述,简述日志数据采集使用到了哪些技术框架 

   A.日志的数据采集使用flume source.type = taildir 监听某个目录下的多个文件，并且实现文件的断点续传功能 注意 flume1.7版本及其以后版本才支持；
     拦截器类型使用静态拦截器 interceptors.type = static  为的是采集不同的数据进入不同的topic;
     sinks.type =avro  sink端的avro是一个数据发送者;
     
   B.一共三个flume 两个采集 （一个采集海口的订单日志数据、一个采集成都的轨迹日志数据）把采集到的数据 下沉到 第三个flume 
     第三个 flume source.type =avro  source中的avro组件是一个接收者服务;
           sinks.type = org.apache.flume.sink.kafka.KafkaSink 数据下沉到kafka中
           sinks.topic = %{type} 根据静态拦截器 把数据分发到对应的topic中
    
   C.数据到了kafka 中 
     使用 SparkStreaming 来消费 kafka 中数据 1.0直连的方式  
     KafkaUtils.createDirectStream(streamingContext, LocationStrategies.PreferConsistent, consumerStrategy)
     并手动维护offset到Hbase中去
     设计到Hbase中 存放offset 表的设计 
         * 设计到 Hbase 表的设计   表名
          * 列族 f1 ：          存在多个列  消费者组：主题 ：分区
          *                    group1：topic：partition1     group1：topic：partition1   group2：topic：partition2     group2：topic：partition2
          * rowkey：消费者组：主题
          * group1：topic         long 25535                  long 25535
          * group2：topic  
     consumerStrategy的 确定问题  查询 hbase 中是否已经 存在数据 来确定 是否传入 Map<partiton,Long>
     消费数据到存入Hbase和redis 最后手动维护offset的值到hbase中
   D.用到flume、kafka 、Hbase、SparkStreaming、redis技术框架 
(2)描述mysql数据库当中的数据如何进行实时采集,遇到了哪些问题,如何解决的
    A.使用maxwell实现解析binlog，并将binlog数据接入到kafka当中去
    B.解决 maxwell 数据倾斜问题 
     maxwell配置文件
         kafka_partition_hash=murmur3
         producer_partition_by=primary_key
(3)简述 Hbase的rowkey的设计技巧,如何解决热点问题
  问题叙述：没有设计hbase的rowkey，也没有做hbase的预分区的操作 ，导致数据一直进入到hbase里面去 ===》 产生region的频繁的分裂的问题 ==》 产生热点数据的问题
  问题解决：
         设计原则 预分区与rowkey的设计要紧密的结合起来  rowkey的设计，与后续的查询紧密相关的  
  预分区的设计：使用了 最大的字典字符 | 来进行隔离后面的字符串
  设计了8个预分区 0001| 0002| 0003| 0004| 0005| 0006| 0007| 0008| 0009|
  rowkey的设计 使用了16位长度 前四位 正常的进行获取 （ id+"_" + 时间戳 ）.hashCode % numRegions ===》得到 0-7的数字，进行左补全  
  再通过 （id+"_" + 时间戳）.md5加密 截取12位 拼接上前4位 就得到了最后的16位的rowkey的长度
  主键是自增的 ==》 每次通过 主键.hashCode % numregions 得到的值，也是比较均衡的

(4)简述 sparKSQl如何自定义数据源实现异数据源的sql查询
 //1.创建SparkSession
 val spark = SparkSession.builder().master("local[2]").getOrCreate()
 //2.spark.read.format 自定义数据源  查询HBASE的数据 将 HBASE的数据 映射成为一张表
 val df = spark.read.format("com.travel.programApp.HBaseSource") 
      .option("hbase.table.name", "spark_hbase_sql")  // 表名
      .option("schema", "`name` STRING,`score` STRING") //表结构
      .option("cf.cc", "cf:name,cf:score") // 列名
      .load() 
 //3.HBaseSource 继承 DataSourceV2 和 ReadSupport 
 class HBaseSource extends DataSourceV2 with ReadSupport
 //4. 实现 createReader 方法 并返回 DataSourceReader 
 override def createReader(options: DataSourceOptions): DataSourceReader 
 //5. 自定义 HBaseDataSourceReader 类 继承 DataSourceReader 
 class HBaseDataSourceReader(tableName: String, schema: String, cfcc: String) extends DataSourceReader 
 //6.实现 createDataReaderFactories 方法 返回 List[DataReaderFactory]
 override def createDataReaderFactories(): util.List[DataReaderFactory[Row]] 
 //7.自定义 HBaseReaderFactory 类 继承 DataReaderFactory 
 class HBaseReaderFactory(tableName: String, cfcc: String) extends DataReaderFactory[Row]
 //8.实现 createDataReader 方法 返回 DataReader[Row]
 override def createDataReader(): DataReader[Row]
 //9.自定义 HBaseReader 类 继承 DataReader 
 class HBaseReader(tableName: String, cfcc: String) extends DataReader[Row]
 //10.实现 getIterator方法 next() 方法 get() 方法 close() 方法
   A.val data: Iterator[Seq[AnyRef]] = getIterator
   B.override def next()
   C.override def get()
   D.override def close()
 //11. 将返回的dataframe 映射成一张表
  df.createOrReplaceTempView("sparkHBaseSQL")
 //12.执行sql语句
 val frame: DataFrame = spark.sql("select * from sparkHBaseSQL where score > 60")

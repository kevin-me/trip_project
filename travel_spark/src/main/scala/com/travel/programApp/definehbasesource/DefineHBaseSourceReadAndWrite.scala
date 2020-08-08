package com.travel.programApp.definehbasesource

import java.util
import java.util.Optional

import com.travel.utils.HbaseTools
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport, WriteSupport}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

object DefineHBaseSourceReadAndWrite {

  def main(args: Array[String]): Unit = {
    //使用sparksql自定义数据源 直接查询Hbase数据
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .getOrCreate()
    //spark.read.format 自定义数据源  查询HBASE的数据 将 HBASE的数据 映射成为一张表
    /**
     * hbase 的字段  name列 score 列
     * spark_hbase_sql 表名
     *
     */
    val df = spark.read
      .format("com.travel.programApp.HBaseSource")  //自定义读数据源
      .option("hbase.table.name", "spark_hbase_sql")  // 表名
      .option("schema", "`name` STRING,`score` STRING") //表结构
      .option("cf.cc", "cf:name,cf:score") // 列名
      .option("STARTROW","") //      val tmpTime = TimeUtils.formatYYYYmmdd(create_time).get   val rowkey = order.id + "_" + tmpTime
      .option("ENDROW","")  //      PageFilter filter1 = new PageFilter(pageSize);  scan.setFilter(filter1);
      .load()

    //问题  rowkey 的设计是 创建时间   无法指定 确定的 STARTROW和  ENDROW 如果 传入 分页  但是还不支持 PageFilter

    //打印出来执行计划
      df.explain(true)
      df.createOrReplaceTempView("sparkHBaseSQL")
      df.printSchema()
      val frame: DataFrame = spark.sql("select * from sparkHBaseSQL where score > 60")

    //自定义 写数据源
      frame.write.format("com.travel.programApp.HBaseSource")
        .mode(SaveMode.Overwrite)
        .option("hbase.table.name", "spark_hbase_write")
        .save()
  }
}

class HBaseSource extends DataSourceV2 with ReadSupport with WriteSupport {

  override def createReader(options: DataSourceOptions): DataSourceReader = {

    val tableName: String = options.get("hbase_table_name").get()

    val schema: String = options.get("schema").get()

    val cfcc: String = options.get("cf.cc").get()

    new HBaseDataSourceReader(tableName,schema,cfcc)

  }

  override def createWriter(jobId: String, schema: StructType, mode: SaveMode, options: DataSourceOptions): Optional[DataSourceWriter] = {

    Optional.of(new HBaseDataSourceWrite)

  }
}
class HBaseDataSourceWrite extends DataSourceWriter {

  override def createWriterFactory(): DataWriterFactory[Row] = {

    new HBaseDataWriterFactory

  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {

  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {

  }
}

class HBaseDataWriterFactory extends DataWriterFactory[Row] {

  override def createDataWriter(partitionId: Int, attemptNumber: Int): DataWriter[Row] = {

    new HBaseDataWriter

  }
}

class HBaseDataWriter extends DataWriter[Row] {

  private val conn: Connection = HbaseTools.getHbaseConn

  private val table: Table = conn.getTable(TableName.valueOf("spark_hbase_write"))

  override def write(record: Row): Unit = {

    val name: String = record.getString(0)

    val score: String = record.getString(1)

    val put = new Put("0001".getBytes())

    put.addColumn("cf".getBytes(), "name".getBytes(), name.getBytes())

    put.addColumn("cf".getBytes(), "score".getBytes(), score.getBytes())

    table.put(put)
  }

  override def commit(): WriterCommitMessage = {
    table.close()
    conn.close()
    null
  }

  override def abort(): Unit = {
    null
  }
}
class HBaseDataSourceReader(tableName: String, schema: String, cfcc: String) extends DataSourceReader {
  //定义HBase的schema
  private val structType: StructType = StructType.fromDDL(schema)

  override def readSchema(): StructType = {
    structType
  }

  //返回DataReaderFactory
  override def createDataReaderFactories(): util.List[DataReaderFactory[Row]] = {
    //很常用
    import collection.JavaConverters._
    Seq(
      new HBaseReaderFactory(tableName, cfcc).asInstanceOf[DataReaderFactory[Row]]
    ).asJava

  }

}
class HBaseReaderFactory(tableName: String, cfcc: String) extends DataReaderFactory[Row] {

  override def createDataReader(): DataReader[Row] = {

    new HBaseReader(tableName, cfcc)

  }
}

class HBaseReader(tableName: String, cfcc: String) extends DataReader[Row] {

  private var hbaseConnection: Connection = null

  private var resultScanner: ResultScanner = null

  private var nextResult: Result = null

  /**
   * 迭代器方式 不太好想
   */

  val data: Iterator[Seq[AnyRef]] = getIterator

  def getIterator: Iterator[Seq[AnyRef]] = {

    import scala.collection.JavaConverters._

    //获取hbase 的链接
    hbaseConnection = HbaseTools.getHbaseConn

    //
    val table: Table = hbaseConnection.getTable(TableName.valueOf(tableName))

    resultScanner = table.getScanner(new Scan())

    val iterator: Iterator[Seq[AnyRef]] = resultScanner.iterator().asScala.map(eachResult => {

      val str: String = Bytes.toString(eachResult.getValue("cf".getBytes(), "name".getBytes()))

      val score: String = Bytes.toString(eachResult.getValue("cf".getBytes(), "score".getBytes()))

      Seq(str, score)

    })
    iterator
  }

  override def next(): Boolean = {

    data.hasNext

  }
  override def get(): Row = {

    val seq: Seq[Any] = data.next()

    Row.fromSeq(seq)
  }

  override def close(): Unit = {
    hbaseConnection.close()
  }
}


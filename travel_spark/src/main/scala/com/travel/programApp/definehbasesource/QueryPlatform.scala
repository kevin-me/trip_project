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

object QueryPlatform {
  def main(args: Array[String]): Unit = {
    /**
     * sparksql 自定义数据源 查询Hbase数据
     */
    val sparkSession: SparkSession = SparkSession.builder().master("local[2]").getOrCreate()

    val dataFrame: DataFrame = sparkSession.read.format("com.travel.programApp.definehbasesource.HBaseSource")
      .option("hbase.table.name", "spark_hbase_sql")
      .option("schema", "`name` STRING,`score` STRING")
      .option("cf.cc", "cf:name,cf:score").load()

    dataFrame.createOrReplaceTempView("sparkHBaseSQL")

    val frame: DataFrame = sparkSession.sql("select * from sparkHBaseSQL where score > 60")

    //自定义 写数据源
    frame.write.format("com.travel.programApp.definehbasesource.HBaseSource")
      .mode(SaveMode.Overwrite)
      .option("hbase.table.name", "spark_hbase_write")
      .save()
  }

}

class HBaseSource extends DataSourceV2 with ReadSupport with WriteSupport {
  override def createReader(options: DataSourceOptions): DataSourceReader = {
    val tableName: String = options.get("hbase.table.name").get()
    val schema: String = options.get("schema").get()
    val cfcc: String = options.get("cf.cc").get()
    new Hbase2DataSourceReader(tableName, schema, cfcc)
  }

  override def createWriter(s: String, structType: StructType, saveMode: SaveMode, dataSourceOptions: DataSourceOptions): Optional[DataSourceWriter] = {
    Optional.of(new HBaseDataSourceWrite)
  }
}

class HBaseDataSourceWrite extends DataSourceWriter {


  override def createWriterFactory(): DataWriterFactory[Row] = {

    new HbaseDataWriterFactory
  }

  override def commit(writerCommitMessages: Array[WriterCommitMessage]): Unit = {


  }

  override def abort(writerCommitMessages: Array[WriterCommitMessage]): Unit = {


  }
}

class HbaseDataWriterFactory extends DataWriterFactory[Row] {
  override def createDataWriter(i: Int, i1: Int): DataWriter[Row] = {

    new HbaseDataWrite()

  }
}

class HbaseDataWrite extends DataWriter[Row] {
  private val hbaseConn: Connection = HbaseTools.getHbaseConn

  private val table: Table = hbaseConn.getTable(TableName.valueOf("spark_hbase_write"))

  override def write(record: Row): Unit = {

    val name: String = record.getString(0)

    val score: String = record.getString(1)

    val put = new Put("0001".getBytes())

    put.addColumn("cf".getBytes(), "name".getBytes(), name.getBytes())

    put.addColumn("cf".getBytes(), "score".getBytes(), score.getBytes())

    table.put(put)
  }

  override def commit(): WriterCommitMessage = {

    hbaseConn.close()
    table.close()
    null

  }

  override def abort(): Unit = {

  }
}

class Hbase2DataSourceReader(tableName: String, schema: String, cfcc: String) extends DataSourceReader {

  //定义HBase的schema
  private val structType: StructType = StructType.fromDDL(schema)

  override def readSchema(): StructType = {
    structType
  }

  override def createDataReaderFactories(): util.List[DataReaderFactory[Row]] = {
    //很常用
    import collection.JavaConverters._
    Seq {
      new HbaseDataReaderFactory(tableName, cfcc).asInstanceOf[DataReaderFactory[Row]]
    }.asJava

  }
}

class HbaseDataReaderFactory(tableName: String, cfcc: String) extends DataReaderFactory[Row] {

  override def createDataReader(): DataReader[Row] = {

    new HbaseDataReader(tableName, cfcc)

  }
}

class HbaseDataReader(tableName: String, cfcc: String) extends DataReader[Row] {

  private var hbaseConn: Connection = null

  private var scanner: ResultScanner = null

  /**
   * 迭代器的思想 不太好想
   *
   * @return
   */
  val data: Iterator[Seq[AnyRef]] = getIterator

  def getIterator: Iterator[Seq[AnyRef]] = {

    import scala.collection.JavaConverters._

    //获取Hbase的数据放入迭代器
    hbaseConn = HbaseTools.getHbaseConn

    val table: Table = hbaseConn.getTable(TableName.valueOf(tableName))

    scanner = table.getScanner(new Scan())

    val iterator: Iterator[Seq[String]] = scanner.iterator().asScala.map(eachResult => {

      val name: String = Bytes.toString(eachResult.getValue("cf".getBytes(), "name".getBytes()))
      val score: String = Bytes.toString(eachResult.getValue("cf".getBytes(), "score".getBytes()))

      Seq(name, score)

    })
    iterator
  }

  override def next(): Boolean = {
    data.hasNext
  }

  override def get(): Row = {
    val seq: Seq[AnyRef] = data.next()
    Row.fromSeq(seq)
  }

  override def close(): Unit = {
    hbaseConn.close()
  }
}

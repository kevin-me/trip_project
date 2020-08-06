package com.travel.programApp

import java.util

import com.alibaba.fastjson.JSONArray
import com.travel.bean.HaiKouOrder
import com.travel.common.{Constants, District, MapUtil}
import com.travel.utils.SparkUtils
import com.uber.h3core.H3Core
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.api.java.UDF3
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.locationtech.jts.geom.{Point, Polygon}
import org.locationtech.jts.io.WKTReader

import scala.collection.mutable

object VirtualStation {

  def main(args: Array[String]): Unit = {


    val sparkConf: SparkConf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").setAppName("virtualStation").setMaster("local[2]")

    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val sparkContext: SparkContext = sparkSession.sparkContext

    sparkContext.setLogLevel("WARN")

    val configuration: Configuration = HBaseConfiguration.create()

    configuration.set("hbase.zookeeper.quorum", "node01,node02,node03")

    configuration.set("hbase.zookeeper.property.clientPort", "2181")

    configuration.setInt("hbase.client.operation.timeout", 3000)

    configuration.set("hbase.mapreduce.inputtable", "HTAB_HAIKOU_ORDER")

    // 1.读取hbase中的数据
    val scan = new Scan()
    scan.addFamily(Bytes.toBytes("f1"))
    scan.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("ORDER_ID"))
    scan.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("CITY_ID"))
    scan.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("STARTING_LNG"))
    scan.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("STARTING_LAT"))

    configuration.set(TableInputFormat.SCAN, SparkUtils.convertScanToString(scan))

    val haikouRdd: RDD[(ImmutableBytesWritable, Result)] = sparkContext.newAPIHadoopRDD(configuration, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

    import sparkSession.implicits._

    val ordersRdd: RDD[HaiKouOrder] = haikouRdd.mapPartitions(eachPartition => {

      val orders: Iterator[HaiKouOrder] = eachPartition.map(eachResult => {

        val line: Result = eachResult._2
        val order_id: String = Bytes.toString(line.getValue(Bytes.toBytes("f1"), Bytes.toBytes("ORDER_ID")))
        val city_id: String = Bytes.toString(line.getValue(Bytes.toBytes("f1"), Bytes.toBytes("CITY_ID")))
        val starting_lng: String = Bytes.toString(line.getValue(Bytes.toBytes("f1"), Bytes.toBytes("STARTING_LNG")))
        val starting_lat: String = Bytes.toString(line.getValue(Bytes.toBytes("f1"), Bytes.toBytes("STARTING_LAT")))
        HaiKouOrder(order_id, city_id, starting_lng, starting_lat)

      })
      orders
    })
    val dataFrame: DataFrame = ordersRdd.toDF()

    dataFrame.createOrReplaceTempView("order_df")

    // 2.自定义udf函数  计算 经纬度的 h3算法的值   求出 虚拟车站

    val h3 = H3Core.newInstance

    sparkSession.udf.register("locationToH3", new UDF3[String, String, Int, Long] {
      override def call(lat: String, lng: String, result: Int): Long = {

        h3.geoToH3(lat.toDouble, lng.toDouble, result)

      }
    }, DataTypes.LongType)

    // 定义一个sql
    val order_sql =
      s"""
         |select
         |order_id,
         |city_id,
         |starting_lng,
         |starting_lat,
         |locationToH3(starting_lat,starting_lng,12) as  h3code
         |from order_df
         |""".stripMargin

    val gridDf: DataFrame = sparkSession.sql(order_sql)

    gridDf.createOrReplaceTempView("order_grid")

    val sql =
      s"""
         |select
         |order_id,
         |city_id,
         |starting_lng,
         |starting_lat,
         |row_number() over(partition by order_grid.h3code order by starting_lng,starting_lat asc) rn
         |from order_grid join (
         | select h3code,count(1) as totalResult from order_grid  group by h3code having totalResult >=1
         | ) groupcount on order_grid.h3code = groupcount.h3code
         |having(rn=1)
         |
         |""".stripMargin

    val virtual_frame: DataFrame = sparkSession.sql(sql)

    val virtual_rdd: RDD[Row] = virtual_frame.rdd

    //每一个虚拟车站，对应是属于哪一个区域的
    //继续双层循环遍历，每一个虚拟车站与每一个区进行比较，比较虚拟车站是属于哪一个区的


    //每一个虚拟车站的点，需要与每一个地区进行比较
    //求出每一个地区的边界值的，进行广播出去

    val districtList = new java.util.ArrayList[District]()

    //获取海口市下面每个行政区的边界
    val districts: JSONArray = MapUtil.getDistricts("海口市", null);

    MapUtil.parseDistrictInfo(districts, null, districtList);


    val districtsBroadcastVar: Broadcast[util.ArrayList[District]] = sparkContext.broadcast(districtList)

    // 使用java的 GemMetry  判断点和面的 对应关系
    virtual_rdd.mapPartitions(eachPartition => {

      //将边界区域表示成为一个图形类
      //使用JTS-Tools来通过多个经纬度，画出多边形
      import org.geotools.geometry.jts.JTSFactoryFinder
      val geometryFactory = JTSFactoryFinder.getGeometryFactory(null)
      var reader = new WKTReader(geometryFactory)

      //获取广播变量
      val districtList = districtsBroadcastVar.value
      //将java集合转换成为scala集合，或者将scala集合转换成为java集合
      import scala.collection.JavaConversions._

      val wktPolygons: mutable.Buffer[(District, Polygon)] = districtList.map(district => {
        val polygonStr = district.getPolygon
        var wktPolygon = ""
        if (!StringUtils.isEmpty(polygonStr)) {
          wktPolygon = "POLYGON((" + polygonStr.replaceAll(",", " ").replaceAll(";", ",") + "))"
          val polygon: Polygon = reader.read(wktPolygon).asInstanceOf[Polygon]
          (district, polygon)
        } else {
          null
        }
      })
      eachPartition.map(row => {
        val lng: String = row.getAs[String]("starting_lng")
        val lat: String = row.getAs[String]("starting_lat")

        val wktPoint = "POINT(" + lng + " " + lat + ")"
        val point: Point = reader.read(wktPoint).asInstanceOf[Point]

        //循环遍历每一个区域的多边形
        val rows: mutable.Buffer[Row] = wktPolygons.map(polygon => {
          if (polygon._2.contains(point)) {
            val fields: Array[Any] = row.toSeq.toArray ++ Seq(polygon._1.getName)
            Row.fromSeq(fields)
          } else {
            null
          }

        }).filter(null != _)
        rows

      })


    })


  }

}

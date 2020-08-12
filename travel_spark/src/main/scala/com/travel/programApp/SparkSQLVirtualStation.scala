package com.travel.programApp

import java.util

import com.travel.common.{Constants, District}
import com.travel.spark.SparkEngine
import com.travel.utils.{HbaseTools, SparkUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.locationtech.jts.geom.{Point, Polygon}
import org.locationtech.jts.io.WKTReader

import scala.collection.mutable


object SparkSQLVirtualStation {

  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf
//    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//    conf.setMaster("local[1]").setAppName("sparkHbase")

    //获取到了sparkConf
    val conf :SparkConf = SparkEngine.getSparkConf()

    val sparkSession: SparkSession = SparkSession.builder()
      .config(conf)
      .config("spark.extraListeners","com.travel.listener.SparkSessionListener")
      .getOrCreate()

    //设置日志级别，避免出现太多日志信息
    sparkSession.sparkContext.setLogLevel("WARN")
    //hbase配置
    val hconf: Configuration = HBaseConfiguration.create()
    hconf.set("hbase.zookeeper.quorum", "node01,node02,node03")
    hconf.set("hbase.zookeeper.property.clientPort", "2181")
    hconf.setInt("hbase.client.operation.timeout", 3000)
    //spark加载hbase的数据

    val hbaseFrame: DataFrame = HbaseTools.loadHBaseData(sparkSession, hconf)
    //order_id, city_id, starting_lng, starting_lat
    hbaseFrame.createOrReplaceTempView("order_df")


    //获取到了每一个虚拟的车站
    val virtual_rdd: RDD[Row] = SparkUtils.getVirtualFrame(sparkSession)

    //每一个虚拟车站，对应是属于哪一个区域的
    //继续双层循环遍历，每一个虚拟车站与每一个区进行比较，比较虚拟车站是属于哪一个区的


    //每一个虚拟车站的点，需要与每一个地区进行比较
    //求出每一个地区的边界值的，进行广播出去
    val districtBroadCastVar: Broadcast[util.ArrayList[District]] = SparkUtils.broadCastDistrictValue(sparkSession)

    virtual_rdd.mapPartitions(eachPartition =>{
      //将边界区域表示成为一个图形类
      //使用JTS-Tools来通过多个经纬度，画出多边形
      import org.geotools.geometry.jts.JTSFactoryFinder
      val geometryFactory = JTSFactoryFinder.getGeometryFactory(null)
      var reader = new WKTReader(geometryFactory)
      //将哪一个区的，哪一个边界求出来
      val wktPolygons: mutable.Buffer[(District, Polygon)] = SparkUtils.changeDistictToPolygon(districtBroadCastVar, reader)
      //获取到了每一个虚拟车站的点
      eachPartition.map(row =>{
        val lng: String = row.getAs[String]("starting_lng")
        val lat: String = row.getAs[String]("starting_lat")

        val wktPoint = "POINT(" +  lng + " " + lat + ")"
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

    //通过经纬度来计算哪些经纬度是属于同一个六边形的  ===》 10个经纬度属于同一个六边形的  ==》 从这10个经纬度里面选择一个作为虚拟车站的定位
    //通过经纬度  使用H3的算法  得到一个值，相同的值，属于同一个经纬度   自定义udf函数，传入经纬度，给我传出一个结果只
    //需要三个参数，第一二个是经纬度，第三个是六边形精确的范围值

  /*  val h3: H3Core = H3Core.newInstance()
    sparkSession.udf.register("locationToH3",new UDF3[String,String,Int,Long] {
      override def call(lat: String, lng: String, result: Int): Long = {
        h3.geoToH3(lat.toDouble,lng.toDouble,result)
      }
    },DataTypes.LongType)


    val  order_sql =
      """
        |select
        | order_id,
        | city_id,
        | starting_lng,
        | starting_lat,
        | locationToH3(starting_lat,starting_lng,12) as h3code
        |from order_df
        |""".stripMargin


    //将每一个经纬度转换成为了H3的编码  继续确认哪些H3的编码值是相同的，进行分组即可
    val gridDF: DataFrame = sparkSession.sql(order_sql)
    gridDF.createOrReplaceTempView("order_grid")

    val sql: String =
      s"""
         | select
         |order_id,
         |city_id,
         |starting_lng,
         |starting_lat,
         |row_number() over(partition by order_grid.h3code order by starting_lng,starting_lat asc) rn
         | from order_grid  join (
         | select h3code,count(1) as totalResult from order_grid  group by h3code having totalResult >=1
         | ) groupcount on order_grid.h3code = groupcount.h3code
         |having(rn=1)
      """.stripMargin
    //上面的sql语句，将每个经纬度转换成为了一个HashCode码值，然后对hashCode码值分组，获取每个组里面经纬度最小的那一个，得到这个经纬度，然后再计算，这个经纬度坐落在哪一个区里面
    val virtual_frame: DataFrame = sparkSession.sql(sql)
    //判断每个虚拟车站一个经纬度，这个经纬度坐落在哪一个区里面，就能知道每个区里面有多少个虚拟车站了
    val virtual_rdd: RDD[Row] = virtual_frame.rdd
    virtual_rdd*/







  }


}

package com.sqq.main

import java.util.concurrent.TimeUnit

import com.mongodb.client.model.{IndexModel, IndexOptions, Indexes}
import com.sqq.main.mongoCon.MongoCon
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object sqqMain {
  val appName = "sqqApp"
  val dbName = "sqqTest"
  val collectionName = "sqq"
  val mongoDBList = "127.0.0.1:27017"
  //这里假定只有一个主题
  val kafkaTopic = "sqqTopic"
  val kafkaGroupId = "sqqGroupA"

  def main(args: Array[String]): Unit = {
    val duration = 5
    val sparkConfig = new SparkConf().setAppName(appName)
      //设置为 true，这样可以保证在 driver 结束前处理完所有已经接受的数据
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      //反压机制；如果目前系统的延迟较长，Receiver端会自动减小接受数据的速率，避免系统因数据积压过多而崩溃
      .set("spark.streaming.backpressure.enabled", "true")
      //在反压机制开启的情况下，限制第一次批处理应该消费的数据，因为程序冷启动 队列里面有大量积压，防止第一次全部读取，造成系统阻塞
      //默认是读取所有
      .set("spark.streaming.backpressure.initialRate", "200")
      //（整数） 默认直接读取所有
      //限制每秒每个消费线程读取每个kafka分区最大的数据量
      .set("spark.streaming.kafka.maxRatePerPartition", "200")

    //每隔5秒采集一次数据
    val sc = new StreamingContext(sparkConfig, Seconds(duration))

    //设置数据库参数
    val mongo = MongoCon("",dbName,mongoDBList)
    //连接数据库,并做一些初始化工作
    mongo.databaseWrite(db =>{
      //获取现有表
      //val tables = db.listCollectionNames().asScala

      //数据库表的创建不需要专门执行create，只要执行插入即可。此处是为了创建索引
      //创建表 sqq
      db.createCollection(collectionName)

      import scala.collection.JavaConverters._
      //创建索引，数据保留2天
      db.getCollection(collectionName).createIndexes(List(
        new IndexModel(Indexes.compoundIndex(Indexes.descending("_id"), Indexes.ascending("Name"))),
        new IndexModel(Indexes.descending("time"), new IndexOptions().expireAfter(3600 * 24 * 2L, TimeUnit.SECONDS))).asJava)
    })



    sc.start()
    sc.awaitTermination()
  }
}
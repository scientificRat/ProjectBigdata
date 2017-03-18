package service

import javautils.DBHelper

import dao._
import domain.{RawRealTimeAd, UserADVisitRecord}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.streaming.kafka.KafkaUtils


/**
  * Created by scientificRat on 2017/3/15.
  */


/**
  * Constants Zone
  *
  * 常量定义区，这些常量不是全局共享变量，
  * 为提高模块内聚性没有写到全局的常量区
  */
object RealTimeADStatService extends Serializable {
    val DEFAULT_ZK_QUORUM = "192.168.242.201:2181,192.168.242.202:2181,192.168.242.203:2181"
    val DEFAULT_CONSUMER_GROUP_ID = "testGroup"
    val DEFAULT_PER_TOPIC_PARTITIONS = Map("AdRealTimeLog" -> 1)
    val DEFAULT_BATCH_DURATION = Seconds(5)
    val DEFAULT_CHECKPOINT_ADDRESS = "hdfs://192.168.242.201:9000/sparkstream"
}


/**
  *
  * @param sparkContext      sparkContext
  * @param zkQuorum          Zookeeper quorum (hostname:port,hostname:port,..)
  * @param groupID           The group id for this consumer
  * @param topics            Map of (topic_name -> numPartitions) to consume. Each partition is consumed
  *                          in its own thread
  * @param bachDuration      The bach duration time
  * @param checkPointAddress The checkPoint address, it's usually a directory of hadoop HDFS
  */
class RealTimeADStatService(@transient sparkContext: SparkContext,
                            zkQuorum: String = RealTimeADStatService.DEFAULT_ZK_QUORUM,
                            groupID: String = RealTimeADStatService.DEFAULT_CONSUMER_GROUP_ID,
                            topics: Map[String, Int] = RealTimeADStatService.DEFAULT_PER_TOPIC_PARTITIONS,
                            bachDuration: Duration = RealTimeADStatService.DEFAULT_BATCH_DURATION,
                            checkPointAddress: String = RealTimeADStatService.DEFAULT_CHECKPOINT_ADDRESS)
    extends Thread with Serializable {

    // 创建streamContext对象
    @transient
    private val streamContext = new StreamingContext(sparkContext, Seconds(5))

    private val kafkaStream = KafkaUtils.createStream(streamContext, zkQuorum, groupID, topics)


    /**
      * 线程主体
      * 更新黑名单数据, 过滤黑名单, 实时计算每天各省各城市各广告的点击量, 实时各个广告最近1小时内各分钟的点击量, 统计top3
      */
    override def run(): Unit = {
        // 标准化输入数据
        val formattedStream = getFormattedStream(kafkaStream)
        formattedStream.cache()
        // 更新黑名单数据
        updateBlackList2(formattedStream)
        // 过滤黑名单
        val filteredStream = getFilteredStream2(formattedStream)
        filteredStream.cache()
        // 实时计算每天各省各城市各广告的点击量((dateOfDay,province,city,advertisementID),visitTime) 并更新到mysql
        doADStatOfEveryDayEveryProvinceEveryCity(filteredStream)
        // 实时各个广告最近1小时内各分钟的点击量 并写入mysql
        doADStatOfRecentHour(filteredStream)
        // 统计top3
        startTop3Stat()
        streamContext.start()
        streamContext.awaitTermination()
    }

    /**
      * 开启统计top3的独立线程
      */
    private def startTop3Stat(): Unit = {
        //开启计算top3线程
        new Thread(new Runnable {
            override def run(): Unit = {
                val dbConnection = DBHelper.getDBConnection()
                val repository = new EverydayTop3ADRepository(dbConnection)
                while (true) {
                    repository.doJob()
                    Thread.sleep(100)
                }
                dbConnection.close()
            }
        }).start()
    }

    /**
      * 标准化输入数据
      *
      * @param dStream 数据
      * @return
      */
    private def getFormattedStream(dStream: DStream[(String, String)]): DStream[RawRealTimeAd] = {
        dStream.map(tp => {
            // 0时间 1省份 2城市 3userID 4adID
            val attrs = tp._2.split("\t")
            new RawRealTimeAd(attrs(0).toLong, attrs(1), attrs(2), attrs(3), attrs(4))
        })
    }

    /**
      * 更新黑名单数据
      *
      * @param dStream 数据
      */
    @deprecated
    private def updateBlackList(dStream: DStream[RawRealTimeAd]): Unit = {
        dStream.map(ad => ((ad.getDateOfDayStr, ad.getUserID, ad.getAdvertisementID), 1L))
            .reduceByKey(_ + _).foreachRDD(rdd => {
            rdd.foreach(tp => {
                val dbConnection = DBHelper.getDBConnection()
                val userADVisitRecordRepository = new UserADVisitRecordRepository(dbConnection)
                userADVisitRecordRepository.insertOrUpdateOnExist(tp._1._1, tp._1._2, tp._1._3, tp._2)
                dbConnection.close()
            })
        })
    }

    /**
      * 更新黑名单数据 第二种实现方法
      * must be used with getFilteredStream2()
      *
      * @param dStream 数据
      */
    private def updateBlackList2(dStream: DStream[RawRealTimeAd]): Unit = {
        // 建立checkpoint
        streamContext.checkpoint(checkPointAddress)
        // 形成新的用户访问数据
        val userVisitRecords = dStream.map(ad => ((ad.getDateOfDayStr, ad.getUserID, ad.getAdvertisementID), 1L))
            .updateStateByKey((it: Iterator[((String, String, String), Seq[Long], Option[Long])])
            => {
                it.map(e => (e._1, e._2.sum + e._3.getOrElse(0L)))
            }, new HashPartitioner(streamContext.sparkContext.defaultParallelism), true)
        // 向数据库写入黑名单数据
        userVisitRecords.filter(_._2 >= 100).foreachRDD(rdd => {
            rdd.foreach(ur => {
                println(s"恶意刷单:$ur")
                val dbConnection = DBHelper.getDBConnection()
                val repository = new BlackListRepository(dbConnection)
                repository.insertOrIgnoreOnExist(ur._1._1, ur._1._2)
                dbConnection.close()
            })
        })
    }

    /**
      * 过滤黑名单
      *
      * @param dStream 数据
      * @return
      */
    @deprecated
    private def getFilteredStream(dStream: DStream[RawRealTimeAd]): DStream[RawRealTimeAd] = {
        dStream.map(raw => (raw.getUserID, raw)).transform(rdd => {
            // read backList from  mysql database
            val dbConnection = DBHelper.getDBConnection()
            val userADVisitRecordRepository = new UserADVisitRecordRepository(dbConnection)
            val blackListRdd = sparkContext.parallelize(userADVisitRecordRepository.queryBlackList()).map((_, 1))
            dbConnection.close()
            rdd.subtractByKey(blackListRdd)
        }).map(_._2)
    }

    /**
      * 过滤黑名单 第二种实现方法
      * must be used with updateBlackList2(), updateBlackList will course error
      *
      * @param dStream 数据
      * @return
      */
    private def getFilteredStream2(dStream: DStream[RawRealTimeAd]): DStream[RawRealTimeAd] = {
        dStream.map(raw => (raw.getUserID, raw)).transform(rdd => {
            // read backList from  mysql database
            val dbConnection = DBHelper.getDBConnection()
            val repository = new BlackListRepository(dbConnection)
            val blackListRdd = sparkContext.parallelize(repository.queryBlackList()).map((_, 1))
            dbConnection.close()
            rdd.subtractByKey(blackListRdd)
        }).map(_._2)
    }

    /**
      * 实时计算每天各省各城市各广告的点击量 并更新到mysql
      *
      * @param dStream 数据源
      */
    private def doADStatOfEveryDayEveryProvinceEveryCity(dStream: DStream[RawRealTimeAd]): Unit = {
        // 映射到((dateOfDay,province,city,advertisementID),visitTime)并求和
        val statisticData = dStream.map(raw =>
            ((raw.getDateOfDayStr, raw.getProvince, raw.getCity, raw.getAdvertisementID), 1L)).reduceByKey(_ + _)
        statisticData.foreachRDD(rdd => {
            rdd.foreach(tp => {
                val dbConnection = DBHelper.getDBConnection()
                val adStatDataRepository = new ADStatDataRepository(dbConnection)
                adStatDataRepository.insertOrUpdateOnExist(
                    tp._1._1.asInstanceOf[String],
                    tp._1._2.asInstanceOf[String],
                    tp._1._3.asInstanceOf[String],
                    tp._1._4.asInstanceOf[String], tp._2)
                dbConnection.close()
            })
        })
    }

    /**
      * 实时各个广告最近1小时内各分钟的点击量 并写入mysql
      *
      * @param dStream 数据
      */
    private def doADStatOfRecentHour(dStream: DStream[RawRealTimeAd]): Unit = {
        val recentHourStat = dStream.map(raw => ((raw.getDateOfMinute, raw.getAdvertisementID), 1L))
            .reduceByKeyAndWindow((a: Long, b: Long) => a + b, Seconds(60), Seconds(5))
        recentHourStat.foreachRDD(rdd => {
            rdd.foreach(tp => {
                // 更新数据库
                val dbConnection = DBHelper.getDBConnection()
                val repository = new PerMinuteADVisitRepository(dbConnection)
                repository.insertOrUpdateOnExist(tp._1._1, tp._1._2, tp._2)
                dbConnection.close()
            })
        })
    }
}

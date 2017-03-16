package service

import java.text.SimpleDateFormat
import java.util.Date
import javautils.DBHelper

import dao.{ADStatDataRepository, UserADVisitRecordRepository}
import domain.RawRealTimeAd
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.streaming.kafka.KafkaUtils


/**
  * Created by sky on 2017/3/15.
  */


object RealTimeADStatService {
    val DEFAULT_ZK_QUORUM = "192.168.242.201:2181,192.168.242.202:2181,192.168.242.203:2181"
    val DEFAULT_CONSUMER_GROUP_ID = "testGroup"
    val DEFAULT_PER_TOPIC_PARTITIONS = Map("AdRealTimeLog" -> 1)
    val DEFAULT_BATCH_DURATION = Seconds(5)
}


/**
  * @param zkQuorum Zookeeper quorum (hostname:port,hostname:port,..)
  * @param groupID  The group id for this consumer
  * @param topics   Map of (topic_name -> numPartitions) to consume. Each partition is consumed
  *                 in its own thread
  */
class RealTimeADStatService(sparkContext: SparkContext,
                            zkQuorum: String = RealTimeADStatService.DEFAULT_ZK_QUORUM,
                            groupID: String = RealTimeADStatService.DEFAULT_CONSUMER_GROUP_ID,
                            topics: Map[String, Int] = RealTimeADStatService.DEFAULT_PER_TOPIC_PARTITIONS,
                            bachDuration: Duration = RealTimeADStatService.DEFAULT_BATCH_DURATION) extends Thread {

    // 创建streamContext对象
    private val streamContext = new StreamingContext(sparkContext, Seconds(5))
    private val kafkaStream = KafkaUtils.createStream(streamContext, zkQuorum, groupID, topics)

    override def run(): Unit = {

        val formatedSteram = kafkaStream.map(tp => {
            // 0时间 1省份 2城市 3userID 4adID
            val attrs = tp._2.split("\t")
            val dateOfDay = new SimpleDateFormat("yyyy-MM-dd").format(attrs(0).toLong)
            new RawRealTimeAd(dateOfDay, attrs(1), attrs(2), attrs(3), attrs(4))
        })
        // 1日期 2省份 3城市 4userID 5adID
        formatedSteram.cache()
        // transform
        // 产生/更新用户广告点击表
        formatedSteram.map(ad => ((ad.getDateOfDay, ad.getUserID, ad.getAdvertisementID), 1L)).reduceByKey(_ + _).foreachRDD(rdd => {
            rdd.foreach(tp => {
                val dbConnection = DBHelper.getDBConnection()
                val userADVisitRecordRepository = new UserADVisitRecordRepository(dbConnection)
                userADVisitRecordRepository.insertOrUpdateOnExist(tp._1._1, tp._1._2, tp._1._3, tp._2)
                dbConnection.close()
            })
        })

        //实时计算每天各省各城市各广告的点击量((dateOfDay,province,city,advertisementID),visitTime)
        val filteredStream = formatedSteram.map(raw => (raw.getUserID, (raw.getDateOfDay, raw.getProvince, raw.getCity, raw.getAdvertisementID))).transform(rdd => {
            // read backList from  mysql database
            val dbConnection = DBHelper.getDBConnection()
            val userADVisitRecordRepository = new UserADVisitRecordRepository(dbConnection)
            val blackListRdd = sparkContext.parallelize(userADVisitRecordRepository.queryBlackList()).map((_, 1))
            dbConnection.close()
            rdd.subtractByKey(blackListRdd)
        }).map(tp => (tp._2, 1L))


        val statisticData = filteredStream.reduceByKey(_ + _)
        // 缓存 ((dateOfDay,province,city,advertisementID),visitTime)
        //        statisticData.cache()

        //        statisticData.map(tp => ((tp._1._1, tp._1._2, tp._1._4), tp._2)).reduceByKey(_ + _)
        //            .map(tp => ((tp._1._1, tp._1._2), (tp._1._3, tp._2))).groupByKey().foreachRDD(rdd => {
        //            rdd.foreach(tp => {
        //                val sorted_ads = tp._2.toList.sortWith(_._2 < _._2)
        //                println(tp._1 + s"${sorted_ads(0)},${sorted_ads(1)},${sorted_ads(3)}")
        //
        //            })
        //        })

        //
        statisticData.foreachRDD(rdd => {
            rdd.foreach(tp => {
                val dbConnection = DBHelper.getDBConnection()
                val adStatDataRepository = new ADStatDataRepository(dbConnection)
                adStatDataRepository.insertOrUpdateOnExist(tp._1._1, tp._1._2, tp._1._3, tp._1._4, tp._2)
                dbConnection.close()
            })
        })


        //        statisticData.print()

        streamContext.start()
        streamContext.awaitTermination()
    }


}

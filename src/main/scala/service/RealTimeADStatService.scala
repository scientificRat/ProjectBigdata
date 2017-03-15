package service

import java.text.SimpleDateFormat
import java.util.Date
import javautils.DBHelper

import dao.UserADVisitRecordRepository
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
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

        val dbConnection = DBHelper.getDBConnection()

        // transform
        // 过滤黑名单
        val filteredStream = kafkaStream.map(tp => {
            // 0时间 1省份 2城市 3userID 4adID
            val attrs = tp._2.split("\t")
            val time = new Date(attrs(0).toLong)
            val date_yyMMdd = new SimpleDateFormat("yy-MM-dd").format(time)
            ((date_yyMMdd, attrs(3), attrs(4)), 1L)
        }).filter(tp => {
            val userADVisitRecordRepository = new UserADVisitRecordRepository()
            !userADVisitRecordRepository.queryIsBlack(tp._1._1, tp._1._2, tp._1._3)
        })



        filteredStream.reduceByKey(_ + _).foreachRDD(rdd => {
            rdd.foreach(tp => {
                val dateOfDay = tp._1._1
                val userID = tp._1._2
                val adID = tp._1._3
                val visitedTime = tp._2
                val userADVisitRecordRepository = new UserADVisitRecordRepository()
                val rst = userADVisitRecordRepository.queryVisitTime(userID, adID, dateOfDay)
                if (rst.isNone) {
                    userADVisitRecordRepository
                        .add(userID, adID, dateOfDay, visitedTime)
                } else {
                    userADVisitRecordRepository
                        .updateVisitTime(userID, adID, dateOfDay, visitedTime + rst.getValue)
                }
            })
        })

        streamContext.start()
        streamContext.awaitTermination()
    }
}

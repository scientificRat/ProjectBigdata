package session

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
        kafkaStream.print()
        streamContext.start()
        streamContext.awaitTermination()
    }
}

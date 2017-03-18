package acc

import domain.{LongRange, SessionRecord}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


object AggregationStatistics {

    val DEFAULT_TIME_BASE_RANGE = Array(new LongRange(1, 3), new LongRange(4, 6), new LongRange(7, 9), new LongRange(10, 30), new LongRange(30, 60))
    val DEFAULT_STEP_BASE_RANGE = Array(new LongRange(1, 3), new LongRange(4, 6))

    def aggregationStatics(sc: SparkContext, rdd: RDD[(String, SessionRecord)],
                           timeRanges: Array[LongRange],
                           pageRanges: Array[LongRange]): AggAccVariable = {
        // 自定义
        val acc = sc.accumulable(new AggAccVariable(timeRanges, pageRanges))(AggAccumulatorParam)
        // 累加
        rdd.foreach(tp => {
            acc.add(tp._2)
        })
        acc.value
    }
}
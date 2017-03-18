package scalautils

import domain.SessionRecord
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import session.{StepLengthAccumulator, VisitCountAccumulator}

/**
  * Created by sky on 2017/3/17.
  */
object AggregationStatistics {
    /*var sessionVisitcount2 = sc.accumulator(0);//累加器2 4~6
    var sessionVisitcount3 = sc.accumulator(0);//累加器3 7~9
    var sessionVisitcount4 = sc.accumulator(0);//累加器4 10~30
    var sessionVisitcount5 = sc.accumulator(0);//累加器5 30~60
    var sessionVisitcount6 = sc.accumulator(0);//累加器5 60以上*/
    //var sessionStepLength2 = sc.accumulator(0);    //步长4~6
    //var sessionStepLength3 = sc.accumulator(0);    //步长6以上
    val shit =9
    def aggreStatics(rdd:RDD[(Int, SessionRecord)], sc : SparkContext):String={
        var sessionVisitcount = sc.accumulator("")(VisitCountAccumulator);//累加器1 1~3
        var sessionStepLength = sc.accumulator("")(StepLengthAccumulator);    //步长1~3

        val timerdd = rdd.mapValues(x=>{((x.getTimestamps.max)/1000)-((x.getTimestamps.min)/1000)})
        val calculateTime = timerdd.values.foreach(x=>{sessionVisitcount.add(x.toString)}
        )
        val pagerdd = rdd.mapValues(x=>x.getPageRecord.length)
        val calculateStep= pagerdd.values.foreach(x=>{
            sessionStepLength.add(x.toString)
        })
        val result = sessionVisitcount.value+sessionStepLength.value
        return result
    }
}
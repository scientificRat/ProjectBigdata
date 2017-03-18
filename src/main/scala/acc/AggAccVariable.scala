package acc

import domain.{LongRange, SessionRecord}

import scala.collection.mutable

/**
  * Created by sky on 2017/3/17.
  */
class AggAccVariable(timeRanges: Array[LongRange], pageRanges: Array[LongRange]) extends Serializable {

    val timeBasedAcc = new mutable.HashMap[LongRange, Long]()
    val pageBasedAcc = new mutable.HashMap[LongRange, Long]()

    // initialize
    for (range <- timeRanges) {
        timeBasedAcc.put(range, 0)
    }
    for (range <- pageRanges) {
        pageBasedAcc.put(range, 0)
    }

    def add(o: AggAccVariable): AggAccVariable = {
        o.timeBasedAcc.foreach((tp) => {
            if (this.timeBasedAcc.contains(tp._1)) {
                this.timeBasedAcc(tp._1) += tp._2
            }
            else {
                this.timeBasedAcc += tp
            }
        })

        o.pageBasedAcc.foreach((tp) => {
            if (this.pageBasedAcc.contains(tp._1)) {
                this.pageBasedAcc(tp._1) += tp._2
            }
            else {
                this.pageBasedAcc.put(tp._1, tp._2)
            }
        })

        this
    }

    def addSessionRecord(sessionRecord: SessionRecord): AggAccVariable = {

        // calculate the visit time of a session and convert (ms) --> (s)
        val visitTimeInSecond = (sessionRecord.getTimestamps.max - sessionRecord.getTimestamps.min) / 1000
        val visitStep = sessionRecord.getPageRecord.length

        this.timeBasedAcc.keySet.foreach(r => {
            if (r.contains(visitTimeInSecond)) {
                this.timeBasedAcc(r) += visitTimeInSecond
            }
        })

        this.pageBasedAcc.keySet.foreach(r => {
            if (r.contains(visitStep)) {
                this.pageBasedAcc(r) += visitStep
            }
        })
        this
    }

    override def toString: String = {
        var rst = "Base on time:\n"
        this.timeBasedAcc.foreach(tp => {
            rst += s"${tp._1.getLow}s ~ ${tp._1.getHigh}s: ${tp._2} | "
        })
        rst += "\n Base on step\n"
        this.pageBasedAcc.foreach(tp => {
            rst += s"${tp._1.getLow} ~ ${tp._1.getHigh}: ${tp._2} | "
        })
        rst
    }
}

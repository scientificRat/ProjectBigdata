package acc

import domain.SessionRecord
import org.apache.spark.{AccumulableParam, AccumulatorParam}

/**
  * Created by sky on 2017/3/18.
  */
object AggAccumulatorParam extends AccumulableParam[AggAccVariable,SessionRecord] {

    override def addAccumulator(r: AggAccVariable, t: SessionRecord): AggAccVariable = r.addSessionRecord(t)

    override def addInPlace(r1: AggAccVariable, r2: AggAccVariable): AggAccVariable = r1.add(r2)

    override def zero(initialValue: AggAccVariable): AggAccVariable = initialValue
}

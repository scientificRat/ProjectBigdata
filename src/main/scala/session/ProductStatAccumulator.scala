package session

import domain.ProductStat
import org.apache.spark.AccumulatorParam

/**
  * Created by sky on 2017/3/11.
  */
object ProductStatAccumulator extends AccumulatorParam[ProductStat]{
  override def addInPlace(r1: ProductStat, r2: ProductStat): ProductStat = ???

  override def zero(initialValue: ProductStat): ProductStat = ???
}

package fourquant.imagej

import java.io.Serializable

import ch.fourquant.images.types.ImageStatisticsUDT
import org.apache.spark.sql.types


@types.SQLUserDefinedType(udt = classOf[ImageStatisticsUDT])
case class ImageStatistics(min: Double,mean: Double, stdDev: Double,
                           max: Double, pts: Long) extends Serializable {
  def compareTo(is2: ImageStatistics,cutOff: Double = 1e-5): Boolean = {
    val nf = if ((max-min)>0) {max-min} else {1.0}
    return (
      ((1.0*pts - is2.pts)/pts<=cutOff) &
        ((min - is2.min)/nf<=cutOff) &
        ((max - is2.max)/nf<=cutOff) &
        ((stdDev - is2.stdDev)/(stdDev)<=cutOff) &
        ((mean - is2.mean)/nf<=cutOff)
      )
  }

}




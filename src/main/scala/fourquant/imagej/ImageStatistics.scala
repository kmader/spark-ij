package fourquant.imagej

import java.io.Serializable

import fourquant.imagej.ImgStat.ImageStatisticsUDT
import org.apache.spark.sql.{types, Row}
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.types._


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


/**
 * If it has the same name, some scalac things get angry, probably a bug of some sorts
 * Error:scalac: error while loading ImageStatistics, illegal class file dependency between 'object ImageStatistics' and 'class ImageStatistics'
 */
object ImgStat extends Serializable {
  /**
   * the Sparksql user-defined type for the the ImageStatistics
   */
  class ImageStatisticsUDT extends UserDefinedType[ImageStatistics] {
    /** some serious cargo-cult, no idea hwo this is actually used **/
    override def sqlType: StructType = {
      StructType(
        Seq(
          StructField("min",DoubleType, nullable=false),
          StructField("mean",DoubleType, nullable=false),
          StructField("stdev",DoubleType, nullable=false),
          StructField("max",DoubleType, nullable=false),
          StructField("pts",LongType, nullable=false)
        )
      )
    }

    override def serialize(obj: Any): Row = {
      val row = new GenericMutableRow(5)
      obj match {
        case pData: ImageStatistics =>
          row.setDouble(0,pData.min)
          row.setDouble(1,pData.mean)
          row.setDouble(2,pData.stdDev)
          row.setDouble(3,pData.max)
          row.setLong(4,pData.pts)
        case _ =>
          throw new RuntimeException("The given object:"+obj+" cannot be serialized by "+this)
      }
      row
    }

    override def deserialize(datum: Any): ImageStatistics = {
      datum match {
        case r: Row =>
          require(r.length==5,"Wrong row-length given "+r.length+" instead of 5")

          ImageStatistics(r.getDouble(0),r.getDouble(1),r.getDouble(2),r.getDouble(3),r.getLong(4))
        case _ =>
          throw new RuntimeException("The given object:"+datum+" cannot be deserialized by "+this)
      }

    }

    override def equals(o: Any) = o match {
      case v: ImageStatistics => true
      case _ => false
    }

    override def hashCode = 9571285
    override def typeName = "ImageStatistics["+"]"
    override def asNullable = this

    override def userClass = classOf[ImageStatistics]
  }
}

package ch.fourquant.images.types

import fourquant.imagej.{IJResultsTable, IJHistogram, ImageStatistics}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.types._
/**
 * If it has the same name, some scalac things get angry, probably a bug of some sorts
 * Error:scalac: error while loading ImageStatistics, illegal class file dependency between 'object ImageStatistics' and 'class ImageStatistics'
 */

case class HistogramCC(bin_centers: Array[Double], bin_counts: Array[Int]) {
  def this(hist: IJHistogram) = this(hist.bin_centers,hist.counts)
}


/**
  * the Sparksql user-defined type for the the ImageStatistics
  */
class IJResultsTableUDT extends UserDefinedType[IJResultsTable] {
  //TODO add tests to make sure this is serialized in a sensible manner (also might be useful to restructure field
  // ordering
  /** some serious cargo-cult, no idea hwo this is actually used **/
  override def sqlType: StructType = {
    StructType(
      Seq(
        StructField("header",ArrayType(StringType,false), nullable=false),
        StructField("rows",ArrayType(ArrayType(DoubleType,false),false))
      )
    )
  }

  override def serialize(obj: Any): Row = {
    val row = new GenericMutableRow(5)
    obj match {
      case pData: IJResultsTable =>
        row.update(0,pData.header)
        row.update(1,pData.rows.toArray)
      case _ =>
        throw new RuntimeException("The given object:"+obj+" cannot be serialized by "+this)
    }
    row
  }

  override def deserialize(datum: Any): IJResultsTable = {
    datum match {
      case r: Row =>
        require(r.length==2,"Wrong row-length given "+r.length+" instead of 2")
        IJResultsTable(
          r.getAs[Array[String]]("header"),
          r.getAs[Array[Array[Double]]]("rows")
        )
      case _ =>
        throw new RuntimeException("The given object:"+datum+" cannot be deserialized by "+this)
    }

  }

  override def equals(o: Any) = o match {
    case v: ImageStatistics => true
    case _ => false
  }

  override def hashCode = 8775309
  override def typeName = "IJResultsTable["+"]"
  override def asNullable = this

  override def userClass = classOf[IJResultsTable]
}


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
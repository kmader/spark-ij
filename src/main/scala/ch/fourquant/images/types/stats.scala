package ch.fourquant.images.types

import fourquant.imagej.{IJResultsTable, IJHistogram, ImageStatistics}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{MutableRow, GenericMutableRow}
import org.apache.spark.sql.catalyst.util
import org.apache.spark.sql.catalyst.util.{GenericArrayData, MapData}

import org.apache.spark.sql.types._
/**
 * If it has the same name, some scalac things get angry, probably a bug of some sorts
 * Error:scalac: error while loading ImageStatistics, illegal class file dependency between 'object ImageStatistics' and 'class ImageStatistics'
 */

case class HistogramCC(bin_centers: Array[Double], bin_counts: Array[Int]) {
  def this(hist: IJHistogram) = this(hist.bin_centers,hist.counts)
}

object IJResultsTableUDT extends Serializable {
  def toMap(pData: IJResultsTable) = {
    val keys = pData.header.map(_.asInstanceOf[Any])

    val rows = pData.header.map(rowName => {
      val rowVals = pData.getColumn(rowName) match {
        case Some(iArr) => iArr.toArray
        case None => new Array[Double](0)
      }
      new GenericArrayData(rowVals.map(_.asInstanceOf[Any])).asInstanceOf[Any] // sparksql hates typed arrays
    }) // since arrays are invariant in scala
   util.ArrayBasedMapData(keys,rows)
  }
}

/**
  * the Sparksql user-defined type for the the results table
  */
class IJResultsTableUDT extends UserDefinedType[IJResultsTable] {
  //TODO add tests to make sure this is serialized in a sensible manner (also might be useful to restructure field
  // ordering
  /** some serious cargo-cult, no idea hwo this is actually used **/
  override def sqlType: StructType = {
    StructType(
      Seq(
        StructField("columns",MapType(StringType,ArrayType(DoubleType)))
      )
    )
  }

  override def serialize(obj: Any): InternalRow = {
    val row = new GenericMutableRow(1)
    obj match {
      case pData: IJResultsTable =>
        //TODO stolen from VectorUDT, make a nicer one
        val mapOutput = IJResultsTableUDT.toMap(pData)
          row.update(0, mapOutput)
      case _ =>
        throw new RuntimeException("The given object:"+obj+" cannot be serialized by "+this)
    }
    row
  }

  override def deserialize(datum: Any): IJResultsTable = {
    datum match {
      case r: InternalRow =>
        require(r.numFields==1,"Wrong row-length given "+r.numFields+" instead of 1")
        val outMap = r.getMap(0)

        val header = outMap.keyArray().toArray[String](StringType).asInstanceOf[Array[String]]
        // the arraydata / arraytype methods are horrendous and the data needs to be manually extracted and retyped
        val rows = outMap.valueArray().toArray[ArrayType](ArrayType(DoubleType))
          .asInstanceOf[Array[ArrayType]]
            .map(_.asInstanceOf[util.ArrayData].toDoubleArray())

        IJResultsTable(
          header,
          rows
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

  override def serialize(obj: Any): MutableRow = {
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
      case r: InternalRow =>
        require(r.numFields==5,s"Wrong row-length given ${r.numFields} instead of 5")

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
package ch.fourquant.images.types

import fourquant.imagej.ImagePlusIO.ImageLog
import fourquant.imagej.PortableImagePlus
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.{Row, types}
import org.apache.spark.sql.types.{StringType, StructField, StructType, UserDefinedType}

/**
 * the Sparksql user-defined type for the the PortableImagePlus
 * stores the pip as the log (as plain text json) and the array
 * the array is just an object which will then be recognized by the array to imageplus conversion
 */
class PipUDT extends UserDefinedType[PortableImagePlus] {
  /** some serious cargo-cult, no idea hwo this is actually used **/
  override def sqlType: StructType = {
    StructType(
      Seq(
        StructField("jsonlog",StringType, nullable=false),
        StructField("array",types.BinaryType,nullable=false)
      )
    )
  }

  override def serialize(obj: Any): Row = {
    val row = new GenericMutableRow(2)
    obj match {
      case pData: PortableImagePlus =>
        row.setString(0,pData.imgLog.toJSONString)
        row.update(1,pData.getArray)
        row
      case cRow: Row =>
        System.err.println(s"Something strange happened, or was already serialized: ${cRow}")
        cRow
      case _ =>
        throw new RuntimeException(s"The given class ${obj.getClass.getCanonicalName} containing " +
          s"object: ${obj.toString()} cannot be serialized by ${this.toString}")
    }
  }

  override def deserialize(datum: Any): PortableImagePlus = {
    datum match {
      case v: PortableImagePlus =>
        System.err.println("Something strange happened, or was never serialized")
        v
      case r: Row =>
        require(r.length==2,"Wrong row-length given "+r.length+" instead of 2")
        val ilog = ImageLog.fromJSONString( r.getString(0))
        val inArr = r.getAs[AnyRef](1)
        new PortableImagePlus(Right(inArr),ilog)
    }
  }

  override def equals(o: Any) = o match {
    case v: PortableImagePlus => true
    case _ => false
  }

  override def hashCode = 9571330
  override def typeName = "PortiableImagePlus["+"]"
  override def asNullable = this

  override def userClass: Class[PortableImagePlus] = classOf[PortableImagePlus]
}


/**
 * Created by mader on 10/9/15.
 */
case class NamedSQLImage(sample: String, image: PortableImagePlus)


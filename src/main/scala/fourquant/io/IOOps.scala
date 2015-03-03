package fourquant.io

import fourquant.io.ScifioOps._
import io.scif.img.{ImgOpener, SCIFIOImgPlus}
import net.imglib2.`type`.NativeType
import net.imglib2.`type`.numeric.RealType
import net.imglib2.`type`.numeric.real.{FloatType,DoubleType}
import net.imglib2.`type`.numeric.integer.IntType
import net.imglib2.img.ImgFactory
import net.imglib2.img.array.{ArrayImg, ArrayImgFactory}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

//import net.imglib2.`type`.numeric.real.FloatType
//import net.imglib2.`type`.numeric.integer.IntType

import scala.collection.JavaConversions._

/**
 * A general set of opertions for importing images
 * Created by mader on 2/27/15.
 */
object IOOps {


  implicit class fqContext(sc: SparkContext) extends Serializable {

    private def staticTypeReadImages[T<: RealType[T]](file: String,iFactory: ImgFactory[T],
                                                      iType: T):
    RDD[(String,SCIFIOImgPlus[T])] = {
      sc.binaryFiles(file).mapPartitions{
        curPart =>
          val io = new ImgOpener()
          curPart.flatMap{
            case (filename,pds) =>
              for (img<-io.openPDS[T](filename,pds,iFactory,iType))
              yield (filename,img)
          }
      }
    }

    /**
     * A generic tool for opening images as Arrays
     * @param filepath the path to the files that need to be loaded
     * @param bType a function which creates new FloatType objects and can be serialized
     * @tparam T the primitive type for the array representation of the image
     * @tparam U the imglib2 type for the ArrayImg representation
     * @return a list of pathnames (string) and image objects (SparkImage)
     */
    def genericArrayImages[T,U <: NativeType[U] with RealType[U]](filepath: String,
                                                             bType: () => U)(implicit
                                                                             tm: ClassTag[T]):
    RDD[(String, SparkImage[T,U])] = {
      sc.binaryFiles(filepath).mapPartitions{
        curPart =>
          val io = new ImgOpener()
          curPart.flatMap{
            case (filename,pds) =>
              for (img<-io.openPDS[U](filename,pds,new ArrayImgFactory[U], bType() ))
              yield (filename,
                new GenericSparkImage[T,U](Right(img.getImg.asInstanceOf[ArrayImg[U,_]]),bType)
                )
          }
      }
    }

    /**
     * A version of generic array Images for float-type images
     * @return float-formatted images
     */
    def floatImages(filepath: String): RDD[(String,SparkImage[Float,FloatType])] = 
      sc.genericArrayImages[Float,FloatType](filepath,() => new FloatType)

    /**
     * A version of generic array Images for double-type images
     * @return float-formatted images
     */
    def doubleImages(filepath: String) =
      sc.genericArrayImages[Double,DoubleType](filepath,() => new DoubleType)

    /**
     * A version of generic array Images for float-type images
     * @return float-formatted images
     */
    def intImages(filepath: String) =
      sc.genericArrayImages[Int,IntType](filepath,() => new IntType)
      


  }

}

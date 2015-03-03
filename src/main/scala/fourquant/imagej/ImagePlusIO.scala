package fourquant.imagej

import java.io._

import fourquant.imagej.ParameterSweep.ImageJSweep
import fourquant.imagej.Spiji.{PIPOps, PIPTools}
import ij.ImagePlus
import ij.plugin.PlugIn
import ij.plugin.filter.PlugInFilter
import ij.process.ImageProcessor
import org.apache.spark.annotation.Experimental
import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.collection.mutable.ArrayBuffer

object ImagePlusIO extends Serializable {
  /**
   * Should immutablity of imageplus be ensured at the cost of performance and memory
   */
  val ensureImmutability: Boolean = true




  case class LogEntry(opType: PIPOps,opTool: PIPTools,
                      opVal: String, opArgs: Array[String] = Array.empty[String],
                       children: IndexedSeq[LogEntry] = Array.empty[LogEntry]) {
    def toJSON(): JValue = {
      Map[String,JValue](
        ("optype" -> opType.toString),
        ("optool" -> opTool.toString),
        ("opval" -> opVal),
        ("opargs" -> opArgs.toSeq),
        ("children" -> children.map(_.toJSON))
      )
    }

    def le_eq(le: LogEntry) = {
      (
        (le.opType.toString.equalsIgnoreCase(opType.toString)) &
          (le.opTool.toString.equalsIgnoreCase(opTool.toString)) &
          (le.opVal.equalsIgnoreCase(opVal)) &
          (le.opArgs.mkString(", ").equalsIgnoreCase(opArgs.mkString(", ")))
        )
    }
  }
  import scala.collection.JavaConversions._

  object LogEntry {
    implicit val formats = DefaultFormats

    def apply(opType: PIPOps,opTool: PIPTools,
              opVal: String, opArgs: String): LogEntry = apply(opType,opTool,opVal,Array(opArgs))

    def apply(opType: PIPOps,opTool: PIPTools,
              opVal: String, opArgs: String, oldLog: Array[LogEntry]): LogEntry =
      apply(opType,
      opTool,
      opVal,
      Array(opArgs),
      oldLog)
    def getInfoFromImagePlus(ip: ImagePlus) = {
      val prop = ip.getProperties() match {
        case p if (p!=null) => Some(p)
        case _ => None
      }
      val tprop = prop match {
        case Some(pr) =>
          pr.stringPropertyNames().map(pname => (pname, pr.getProperty(pname))).
            mkString("[", ", ", "]")
        case None => "[]"
      }
      Array("InfoProperty: "+ip.getInfoProperty,
        "Properties: "+tprop,
        "Calibration: "+ip.getCalibration().toString
      )
    }

    def create(fromSrc: String, srcInfo: String) =
      LogEntry(PIPOps.CREATE,PIPTools.OTHER,fromSrc,Array(srcInfo))
    def create(srcImg: ImagePlus) =
      LogEntry(PIPOps.CREATE,PIPTools.IMAGEJ,srcImg.getTitle,getInfoFromImagePlus(srcImg))
    def createFromArray(srcName: String, srcArray: AnyRef) =
      LogEntry(PIPOps.CREATE,PIPTools.OTHER,srcName,Array(srcArray.toString))

    /**
     * record the loading of the images
     * @param loadCmd the command used to load (this is usually the name of the sparkcontext method)
     * @param loadPath the path being loaded (currently as a string)
     * @return a log entry of the loading event
     */
    def loadImages(loadCmd: String, loadPath: String) =
      LogEntry(PIPOps.LOAD,PIPTools.SPARK,loadCmd,loadPath)

    def ijRun(cmd: String, args: String) =
      LogEntry(PIPOps.RUN,PIPTools.IMAGEJ,cmd,Array(args))

    def fromJSON(inJSON: JValue): LogEntry = {

      val optype = PIPOps.valueOf((inJSON \ "optype").extract[String])
      val optool = PIPTools.valueOf((inJSON \ "optool").extract[String])
      val opval = (inJSON \ "opval").extract[String]
      val opargs = (inJSON \ "opargs" \\ classOf[JString]).map(_.toString).toArray
      val children: IndexedSeq[LogEntry] = (inJSON \ "children").
        children.map(fromJSON(_)).toIndexedSeq
      LogEntry(
        optype,optool,opval,opargs,children
      )
    }

    def mergeEntry(mergeName: String, mergeArgs: String, mergeLog: ImageLog) = LogEntry(
      PIPOps.MERGE_STORE,
      PIPTools.SPARK,mergeName,mergeArgs,
      mergeLog.ilog.toArray
    )
  }

  /**
   * A class for keeping track of the transformations applied to an image
   * @param ilog
   */
  case class ImageLog(ilog: ArrayBuffer[LogEntry]) {
    def this(le: LogEntry) = this(ArrayBuffer(le))

    def this(opType: PIPOps, opTool: PIPTools, opVal: String,
             opArgs: Array[String] = Array.empty[String]) =
      this(LogEntry(opType,opTool,opVal,opArgs))

    def this(oldlog: Iterable[LogEntry]) = this(ArrayBuffer.empty[LogEntry] ++ oldlog)
    //TODO this might eventually be a good place to show what has been "queried" from the image
    // for making more traceable 'filter' and 'reduce' commands
    def addComment(comment: String) =
      ilog.append(LogEntry(PIPOps.COMMENT,PIPTools.OTHER, comment))

    /**
     * Add a new entry and create a new imagelog
     * @param le the entry to be added
     * @return a new list
     */
    def appendAndCopy(le: LogEntry) = ImageLog(ilog ++ Array(le) )

    def copy() = ImageLog (ilog.clone() )

    def toJSON(): JValue =
      ilog.map(_.toJSON).toSeq

    def toJsStrArray(): Array[String] =
      ilog.map(i => compact(i.toJSON)).toArray

    def apply(index: Int) = ilog(index)
    def headOption = ilog.headOption

    /**
     * two logs are equal iff they have the same length and are elements are equal
     * @param log2
     * @return
     */
    def log_eq(log2: ImageLog): Boolean = log_eq(log2.ilog)

    def log_eq(log2: Iterable[LogEntry]): Boolean = {
      (
        (ilog.size == log2.size) &
          ilog.zip(log2).map{ case(le1,le2) => le1 le_eq le2 }.reduce(_ & _)
        )
    }

  }


  object ImageLog {
    def merge(logA: ImageLog, logB: ImageLog, opVal: String, opArgs: String): ImageLog = {
      ImageLog(
        ArrayBuffer(
          LogEntry.mergeEntry("A",opArgs,logA),
          LogEntry.mergeEntry("B",opArgs,logB),
          LogEntry(PIPOps.MERGE,PIPTools.SPARK,opVal+"(A+B)",Array(opArgs))
        )
      )
    }
  }

  case class ImageStatistics(min: Double,mean: Double, stdDev: Double,
                             max: Double, pts: Long) extends Serializable {
    def compareTo(is2: ImageStatistics,cutOff: Double = 1e-5): Boolean = {

      if (pts == is2.pts) {
        if ((min == is2.min) & (mean == is2.mean) & (stdDev == is2.stdDev) &
          (max == is2.max)) return true
      } else {
        val nf = if ((max-min)>0) {max-min} else {1}
        return ((min - is2.min)/nf<cutOff) & ((max - is2.max)/nf<cutOff) &
          ((stdDev - is2.stdDev)/(stdDev)<cutOff) & ((mean - is2.mean)/nf<cutOff)
      }
      return false
    }

  }

  val pipStoreSerialization = false
  /**
   * Since ImagePlus is not serializable this class allows for it to be serialized and thus used
   * in operations besides map in Spark.
   * @param baseData either an array of the correct type or an imageplus object
   */
  class PortableImagePlus(var baseData: Either[ImagePlus,AnyRef],
                          var imgLog: ImageLog) extends Serializable {
    /**
     * if only one entry is given, create a new log from the one entry
     * @param logEntry single entry (usually creation)
     */
    def this(bd: Either[ImagePlus,AnyRef], logEntry: LogEntry) =
      this(bd,new ImageLog(logEntry))

    def this(inImage: ImagePlus, oldLog: ImageLog) =
      this(Left(inImage),oldLog)

    @deprecated("this should not be used if imagelog information is available","1.0")
    def this(inImage: ImagePlus) =
      this(inImage,new ImageLog(LogEntry.create(inImage)))



    @deprecated("should not be used, since images should always have a log","1.0")
    def this(inArray: AnyRef) =
      this(Right(inArray), LogEntry.createFromArray("SpijiArray",inArray))

    @deprecated("should only be used when a source is not known","1.0")
    def this(inProc: ImageProcessor) =
      this(
        new ImagePlus(File.createTempFile("img","").getName,inProc)
      )

    def this(inProc: ImageProcessor, oldLog: ImageLog) = this(
      new ImagePlus(File.createTempFile("img","").getName,inProc))

    private def calcImg: ImagePlus =
      baseData match {
        case Left(tImg) => tImg
        case Right(tArr) => Spiji.createImage(File.createTempFile("img","").getName,tArr,false)
      }

    private def calcArray: AnyRef =
      baseData match {
        case Left(tImg) =>
          Spiji.setTempCurrentImage(tImg)
          Spiji.getCurrentImage
        case Right(tArr) => tArr
      }

    lazy val curImg = calcImg
    lazy val curArr = calcArray
    def getImg() = curImg
    def getArray() = curArr

    override def toString(): String = {
      val nameFcn = (inCls: String) => this.getClass().getSimpleName()+"["+inCls+"]"
      baseData.fold(
        img => nameFcn(img.toString),
        arr => nameFcn(scala.runtime.ScalaRunTime.stringOf(arr))
      )
    }

    // useful commands in imagej
    def run(cmd: String, args: String = ""): PortableImagePlus = {
      lazy val pargs = ImageJSweep.parseArgsWithDelim(args," ")
      val localImgCopy = if(ensureImmutability) curImg.duplicate() else curImg

      Spiji.setTempCurrentImage(localImgCopy)
      cmd match {
        case "setThreshold" | "applyThreshold" =>
          import fourquant.imagej.ParameterSweep.ImageJSweep.argMap // for the implicit
          // conversions getDbl
          val lower = pargs.getDbl("lower",Double.MinValue)
          val upper = pargs.getDbl("upper",Double.MaxValue)
          Spiji.setThreshold(lower,upper)
          cmd match {
            case "applyThreshold" =>
              Spiji.run("Convert to Mask")
            case _ =>
              Unit
          }
        case _ =>
          Spiji.run(cmd,args)
      }
      new PortableImagePlus(Spiji.getCurImage(),
        this.imgLog.appendAndCopy(LogEntry.ijRun(cmd,args))
      )
    }

    def runAsPlugin(cmd: String, args: String = ""): Either[PlugIn,PlugInFilter] = {
      Spiji.setTempCurrentImage(curImg)
      Spiji.runCommandAsPlugin(cmd,args) match {
        case plug: PlugIn =>
          Left(plug)
        case plugfilt: PlugInFilter =>
          Right(plugfilt)
      }
    }

    def getImageStatistics() ={
      val istat = curImg.getStatistics
      ImageStatistics(istat.min,istat.mean,istat.stdDev,istat.max,istat.pixelCount)
    }

    def analyzeParticles() = {
      IJResultsTable.fromCL(Some(curImg))
    }

    /**
     * average two portableimageplus objects together
     * @note works for floating point images of the same size
     * @param ip2 second image
     *            @param rescale is the rescaling factor for the combined pixels
     * @return new image with average values
     */
    @Experimental
    def average(ip2: PortableImagePlus,rescale: Double = 2): PortableImagePlus = {
      val outProc = ip2.getImg().getProcessor.
        duplicate().convertToFloatProcessor()
      val curArray = curImg.getProcessor.convertToFloatProcessor().
        getPixels().asInstanceOf[Array[Float]]
      val opixs = outProc.getPixels.asInstanceOf[Array[Float]]
      var i = 0
      while(i<opixs.length) {
        opixs(i)=((opixs(i)+curArray(i))/rescale).toFloat
        i+=1
      }
      outProc.setPixels(opixs)
      new PortableImagePlus(outProc,
        ImageLog.merge(this.imgLog,ip2.imgLog,"AVERAGE","rescale=%f".format(rescale))
      )
    }

    @Experimental
    def multiply(rescale: Double): PortableImagePlus = {
      val outProc = curImg.getProcessor.
        duplicate().convertToFloatProcessor()
      outProc.multiply(rescale)
      new PortableImagePlus(outProc,
        imgLog.appendAndCopy(
          LogEntry(PIPOps.OTHER,PIPTools.SPARK,"multiply",
            "rescale=%f".format(rescale))
        )
      )
    }

    @Experimental
    def subtract(bgImage: PortableImagePlus): PortableImagePlus = {
      val outProc = curImg.getProcessor.duplicate().convertToFloatProcessor()
      val bgArray = bgImage.getImg().getProcessor.convertToFloatProcessor().
        getPixels().asInstanceOf[Array[Float]]
      val opixs = outProc.getPixels.asInstanceOf[Array[Float]]
      var i = 0
      while(i<opixs.length) {
        opixs(i)-=bgArray(i)
        i+=1
      }
      outProc.setPixels(opixs)
      new PortableImagePlus(outProc,
        ImageLog.merge(this.imgLog,bgImage.imgLog,"SUBTRACT","")
      )
    }

    def ++(ip2: PortableImagePlus): PortableImagePlus = {
      val outImg = ip2.getImg().duplicate()
      val outStack = outImg.getImageStack
      val curStack = curImg.getImageStack
      for(i <- 1 to curStack.getSize)
        outStack.addSlice(curStack.getSliceLabel(i),
          curStack.getProcessor(i))
      new PortableImagePlus(outImg,
        ImageLog.merge(this.imgLog,ip2.imgLog,"APPEND","")
      )
    }

    // custom serialization
    @throws[IOException]("if the file doesn't exist")
    private def writeObject(oos: ObjectOutputStream): Unit = {
      oos.writeObject(imgLog)
      oos.writeObject(curArr)
    }
    @throws[IOException]("if the file doesn't exist")
    @throws[ClassNotFoundException]("if the class cannot be found")
    private def readObject(in: ObjectInputStream): Unit =  {
      imgLog = in.readObject.asInstanceOf[ImageLog]
      baseData = Right(in.readObject())
    }
    @throws(classOf[ObjectStreamException])
    private def readObjectNoData: Unit = {
      throw new IllegalArgumentException("Cannot have a dataless PortableImagePlus");
    }
  }

}
/**
 * Created by mader on 1/16/15.
 */
class ImagePlusIO {

}

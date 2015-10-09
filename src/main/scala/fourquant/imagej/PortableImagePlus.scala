package fourquant.imagej

import java.io._

import ch.fourquant.images.types.PipUDT
import fourquant.imagej.ImagePlusIO.{ImageLog, LogEntry}
import fourquant.imagej.ParameterSweep.ImageJSweep
import fourquant.imagej.Spiji.{PIPOps, PIPTools}
import ij.ImagePlus
import ij.plugin.PlugIn
import ij.plugin.filter.PlugInFilter
import ij.process.ImageProcessor
import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.types._


/**
 * Since ImagePlus is not serializable this class allows for it to be serialized and thus used
 * in operations besides map in Spark.
 * @param baseData either an array of the correct type or an imageplus object
 */
@SQLUserDefinedType(udt = classOf[PipUDT])
class PortableImagePlus(var baseData: Either[ImagePlus,AnyRef],
                        var imgLog: ImageLog) extends Serializable {

  val pipStoreSerialization = false
  /**
   * Should immutablity of imageplus be ensured at the cost of performance and memory
   */
  val ensureImmutability: Boolean = true

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

  def getMeanValue() =
    getImageStatistics().mean

  @Experimental
  def analyzeParticles() = {
    IJResultsTable.fromCL(Some(curImg))
  }

  /**
   * Create a histogram for the given image
   * @param range the minimum and maximum values for the histogram
   * @param bins the number of bins
   * @return a histogram case class (compatible with SQL)
   */
  @Experimental
  def getHistogram(range: Option[(Double,Double)] = None, bins: Int = 1000) =
    IJHistogram.fromIJ(Some(curImg),range,bins)

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


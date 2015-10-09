package fourquant.imagej

import ij.ImagePlus
import ij.process.ImageProcessor
import org.apache.spark.annotation.Experimental

/**
 * A class for storing histogram information
 */
case class IJHistogram(bin_centers: Array[Double], counts: Array[Int]) {
  //TODO add sql udt
  def toPairs() = bin_centers.zip(counts)
  override def toString() = {
    ("Hist:\n"+toPairs().filter(_._2>0).map(ij => ij._1+"\t"+ij._2).mkString("\n\t"))
  }
  def interp(newMin: Double, newMax: Double, newCount: Int) = {
    val centStep = (newMax-newMin)/(newCount-1)
    val newCents = newMin.to(newMax,centStep).toArray
    IJHistogram(newCents,IJHistogram.histConverter(bin_centers,counts,newCents))
  }

  /**
   * Histogram subtraction for a simple distance metric
   * @param ij2 the compared distance metric
   * @return the difference between bins normalized by the total number of pixels (0..1)
   */
  @Experimental
  def -(ij2: IJHistogram) = {
    val colMin = (bin_centers++ij2.bin_centers).min
    val colMax = (bin_centers++ij2.bin_centers).max
    val newMe = this.interp(colMin,colMax,IJHistogram.histInterpCount).counts
    val newYou = ij2.interp(colMin,colMax,IJHistogram.histInterpCount).counts
    newMe.zip(newYou).map(ab => Math.abs(ab._1 - ab._2)).sum*1.0/(newMe++newYou).sum
  }
}

object IJHistogram {

  val histInterpCount = 10000

  /**
   * convert one spacing of histogram to another (very discrete)
   * @param recCents
   * @param recCount
   * @param newCents
   * @return
   */
  def histConverter(recCents: Array[Double], recCount: Array[Int], newCents: Array[Double]) = {
    val recPairs = recCents.zip(recCount)
    val centStep = (newCents.max-newCents.min)/newCents.length // assume even spacing

    newCents.map{
      centPos =>
        recPairs.
          filter(rp => Math.abs(rp._1-centPos)<(centStep/2.0)). // keep only the window pts
          map(_._2).sum // keep the count and sum it
    }
  }
  implicit class RobustImageProcessor(ip: ImageProcessor) {
    def getSmartHistogram(cRange: (Double,Double), bins: Int) = {
      ip.setHistogramRange(cRange._1,cRange._2)
      ip.setHistogramSize(bins)
      val histArr = Option(ip.getHistogram()) // java and its shitty npes

      val centStep = (cRange._2-cRange._1)/(bins-1)
      val outCents = cRange._1.to(cRange._2,centStep).toArray
      val outBins = histArr match {
        case Some(properOutput) if (properOutput.length==ip.getHistogramSize()) =>
          properOutput

        case Some(poorlySized) if (poorlySized.length>0) => // resize
          val recBins =
            ip.minValue().to(ip.maxValue(),
              (ip.maxValue()-ip.minValue())/poorlySized.length).toArray
          histConverter(recBins,poorlySized,outCents)
        case None =>
          throw new RuntimeException(
            s"Histogram Returned an Invalid Result:"+histArr
          )
      }

      (outCents,outBins)
    }
  }

  def fromIJ(inImg: Option[ImagePlus] = None, inRange: Option[(Double,Double)] = None,
              bins: Int = 60000) = {
    val cProc = inImg.getOrElse(Spiji.getCurImage).getProcessor.
      duplicate() // don't screw the old one

    val cRange = inRange.getOrElse((cProc.minValue,cProc.maxValue))
    assert(cRange._1<cRange._2,"Histogram maximum should be above minimum")

    val (centArr,countArr) = cProc.getSmartHistogram(cRange,bins)

    assert(cProc.getHistogramMin==cRange._1,"Minimum value is not set properly")
    assert(cProc.getHistogramMax==cRange._2,"Maximum value is not set properly")
    assert(countArr.length==bins, "Bin Count should match output")
    IJHistogram(
      centArr,
      countArr
    )
  }
}

package fourquant.imagej


import ij.{ImagePlus, WindowManager}

/**
 * Created by mader on 1/28/15.
 */
case class IJResultsTable(header: Array[String], rows: IndexedSeq[Array[Double]]) {
  lazy val hmap = header.zipWithIndex.toMap

  def getRowValues(rowId: Int) = {
    rowId match {
      case i if i<numObjects => Some(rows(i).zip(header).map(_.swap).toMap)
      case _ => None
    }
  }
  def getColumn(nm: String) = {
    hmap.get(nm) match {
      case Some(colNum) => Some(rows.map(_(colNum)))
      case None => None
    }
  }

  /**
   * The mean value of a column
   * @param colName
   * @return
   */
  def mean(colName: String) = sum(colName).map(_/numObjects)

  /**
   * The sum of a column (if it exists)
   * @param colName
   * @return
   */
  def sum(colName: String) = getColumn(colName).map(col => (col.sum))

  /**
   * The minimum value of a column (if it exists)
   * @param colName
   * @return
   */
  def min(colName: String) = getColumn(colName).map(col => (col.min))
  /**
   * The maximum value of a column (if it exists)
   * @param colName
   * @return
   */
  def max(colName: String) = getColumn(colName).map(col => (col.max))

  def numObjects = rows.length

  def toString(xrows: Int = 5, xcols: Int = 5) = {
    val subhead = (1 to xcols).zip(header).map(_._2)
    val subrows = (1 to xrows). // keep only the first rows
      zip(rows.map(cr => cr.zip(1 to xcols).map(_._1))). // keep only the first columns
      map(iv => (Array(iv._1.toDouble) ++ iv._2).mkString("\t"))
    ("#"++subhead).mkString("\t")+"\n"+
    subrows.mkString("\n")
  }

  override def toString = toString()

}

object IJResultsTable {
  val measurementsString =
    "area mean standard modal min centroid center perimeter bounding fit shape feret's " +
    "integrated median skewness kurtosis area_fraction stack redirect=None decimal=3"
  def fromCL(cImg: Option[ImagePlus] = None) = {
    cImg.map(WindowManager.setTempCurrentImage(_))
    Spiji.run(
      "Set Measurements...",
      measurementsString
    )
    Spiji.run("Analyze Particles...", "display clear")
    fromIJ()
  }

  def fromIJ() = {
    val header = Spiji.getListColumns()
    val rows = Spiji.getResultsTable().asInstanceOf[Array[Array[Double]]].toIndexedSeq

    IJResultsTable(header,rows)
  }
}

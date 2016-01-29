package fourquant.imagej

import java.io.Serializable

import ch.fourquant.images.types.{IJResultsTableUDT, HistogramCC}
import fourquant.imagej.ImageJSettings
import org.apache.spark.sql.SQLContext

/**
  * Created by mader on 1/25/16.
  */
object SQLFunctions extends Serializable {

  /**
    * The collection of functions added to SQL for dealing with ImageJ functionality. This is made so there will
    * automatically be a documented list of them.
 *
    * @author Kevin Mader
    * @note  TODO add some io and other useful operations here
    * @note TODO add analyze particles
    *
    */
  object udfs extends Serializable {

    /**
      * Run's an imagej plugin (macro style) on an image with arguments
 *
      * @param s the image to process (as [[PortableImagePlus]]
      * @param cmd the plugin or command name to run
      * @param args the arguments to give this plugin
      * @return a new image with the output
      */
    def run2(s: PortableImagePlus, cmd: String, args: String) = s.run(cmd,args)

    /**
      * Run a plugin without arguments on a given image
 *
      * @param s the image to run the plugin on
      * @param cmd the name of the plugin / operation
      * @return a new image after this has been performed (duplicated)
      */
    def run(s: PortableImagePlus, cmd: String) = s.run(cmd)

    /**
      * Run a function on an image and return the ResultsTable
 *
      * @param s the image to process
      * @param cmd the plugin to run
      * @param args the arguments to give it (can be empty)
      * @return the table [[IJResultsTable]] of results after the plugin is run
      */
    def runtable(s: PortableImagePlus, cmd: String, args: String) =
      s.runWithTable(cmd,args)._2

    /**
      * Run a function on an image and return the ResultsTable as a map
      *
      * @param s the image to process
      * @param cmd the plugin to run
      * @param args the arguments to give it (can be empty)
      * @return the table [[IJResultsTable]] as a java map
      */
    def runmap(s: PortableImagePlus, cmd: String, args: String) = {
      s.runWithTable(cmd,args)._2.toMap

    }

    /**
      * Run a function on an image and return the first row of the results table as a map
      *
      * @param s the image to process
      * @param cmd the plugin to run
      * @param args the arguments to give it (can be empty)
      * @return the table [[IJResultsTable]] as a map
      */
    def runrow(s: PortableImagePlus, cmd: String, args: String) = {
      s.runWithTable(cmd,args)._2.getRowValues(0).getOrElse(Map[String,Double]())
    }


    /**
      * Get the statistics of the current image
 *
      * @param s the image
      * @return an [[ImageStatistics]] with the information.
      */
    def stats(s: PortableImagePlus) =
      s.getImageStatistics()

    /**
      * Calculate the mean of an image
 *
      * @param s the current image
      * @return the mean values as double
      *         TODO switch to standard statistics
      */
    def mean(s: PortableImagePlus) = s.getMeanValue()

    /**
      * Run a shape (component-labeling based) on the image
 *
      * @param s
      * @return a string with all the shape information
      *         TODO make shape an udt as well
      * @note see [[PortableImagePlus.analyzeParticles()]]
      */
    def shape(s: PortableImagePlus) = s.analyzeParticles().toString()

    /**
      * Calculates the absolute difference between two images
      * @param s the input image
      * @param t the image to subtract
      * @return a new image [[PortableImagePlus]] with the result
      * @note see [[PortableImagePlus.subtract()]]
      */
    def subtract(s: PortableImagePlus, t: PortableImagePlus) = s.subtract(t)

    /**
      * Scale the image by a factor
 *
      * @param s the image to scale
      * @param scFactor the factor to scale by
      * @return a new image with the scaling
      */
    def scale(s: PortableImagePlus, scFactor: Double) = s.multiply(scFactor)

    /**
      * Calculate the histogram of the given image using an automatically determined bin collection
 *
      * @param s the input image to run on
      * @return a histogram in the form of [[HistogramCC]] taken on the [[PortableImagePlus.getArray()]]
      */
    def hist(s: PortableImagePlus) = new HistogramCC(s.getHistogram())

    /**
      * Calculate a histgram with a fixed bin set on the current image
 *
      * @param s the image to calculate the histogram on
      * @param minVal the minimum value
      * @param maxVal the maximum value
      * @param bins the number of bins
      * @return a histogram of type [[HistogramCC]]
      */
    def hist3(s: PortableImagePlus, minVal: Double, maxVal: Double, bins: Int) =
      new HistogramCC(
        s.getHistogram(
          Some((minVal,maxVal)),
          bins)
    )

    /**
      * Compare the histogram of two images by bin-wise differences
 *
      * @param s1 the base image
      * @param s2 the image to compare it to
      * @return the difference between the bins
      */
    def hist_compare(s1: PortableImagePlus, s2: PortableImagePlus) =
    (s1.getHistogram()-s2.getHistogram())

  }


  /**
    * add all the needed imagej related udfs to the sqlcontext
 *
    * @param sq the sqlcontext to add the functions to
    * @param fs the base imagejsettings to ensure it has been properly initialized
    */
  def registerImageJ(sq: SQLContext, fs: ImageJSettings): Unit = {

    sq.udf.register("run2", (a: PortableImagePlus, cmd: String,args: String) =>  udfs.run2(a,cmd,args))
    sq.udf.register("run", (a: PortableImagePlus, cmd: String) => udfs.run(a,cmd))
    sq.udf.register("runtable", (a: PortableImagePlus, cmd: String, args: String) => udfs.runtable(a,cmd,args))
    sq.udf.register("runmap", (a: PortableImagePlus, cmd: String, args: String) => udfs.runmap(a,cmd,args))
    sq.udf.register("runrow", (a: PortableImagePlus, cmd: String, args: String) => udfs.runrow(a,cmd,args))
    sq.udf.register("stats",(a: PortableImagePlus) => udfs.stats(a))
    sq.udf.register("mean",(a: PortableImagePlus) => udfs.mean(a))
    sq.udf.register("shape",(a: PortableImagePlus) => udfs.shape(a))

    sq.udf.register("subtract",(a: PortableImagePlus, b: PortableImagePlus) => udfs.subtract(a,b))

    sq.udf.register("scale",(a: PortableImagePlus, scf: Double) => udfs.scale(a,scf))

    sq.udf.register("hist",(a: PortableImagePlus) => udfs.hist(a))

    sq.udf.register("hist3",(a: PortableImagePlus, min: Double, max: Double, bins: Int) =>udfs.hist3(a,min,max,bins))

    sq.udf.register("hist_compare",(a: PortableImagePlus, b: PortableImagePlus) => udfs.hist_compare(a,b))

  }


  object debugUdfs extends Serializable {
    /** return the string version of anything that might be put in. Useful for using JDBC clients which do not
      * support more complicated datatypes.
      *
      * @param s any object
      * @return the string representation
      */
    def tostring(s: AnyRef) = s.toString

    /**
      * Show the calibration as a string for the current image
      * @param s the image
      * @return its calibration as a string
      */
    def showcalibration(s: PortableImagePlus) = s"(${s.getCalibration.toString})"


    /**
      * Extract a given column from a table
 *
      * @param s the name of the table
      * @param colName the column to read out
      * @return the column as an array of
      *         @note it will return an empty array of there is none available
      */
    def fromtable(s: IJResultsTable, colName: String) =
      s.getColumn(colName).map(_.toArray).getOrElse(new Array[Double](0))

    import scala.collection.JavaConversions._
    def listplugins()(implicit fs: ImageJSettings) =
      ij.Menus.getCommands.entrySet().toList.map(kv => s"${kv.getKey} => ${kv.getValue}").toArray

    def listcommands()(implicit fs: ImageJSettings) =
      ij.Menus.getCommands.entrySet().toList.map(kv => s"${kv.getKey}").toArray

  }
  /**
    * Add the debugging functions to the context
 *
    * @param sq
    * @return
    */
  def registerDebugFunctions(sq: SQLContext,fs: ImageJSettings) = {
    implicit val ijs = fs
    sq.udf.register("tostring",(a: AnyRef) =>debugUdfs.tostring(a))
    sq.udf.register("fromtable",(a: IJResultsTable, b: String) => debugUdfs.fromtable(a,b))
    sq.udf.register("listplugins", () => debugUdfs.listplugins() )
    sq.udf.register("listcommands", () => debugUdfs.listcommands() )
    sq.udf.register("showcalibration", (s: PortableImagePlus) => debugUdfs.showcalibration(s))
  }

}

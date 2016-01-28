package ch.fourquant.images

import fourquant.imagej.{scOps, TestSupportFcns}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{LocalSparkContext, SparkContext}
import org.scalatest.{FunSuite, Matchers}

/**
 * Created by mader on 10/8/15.
 */
class DDLTests extends FunSuite with Matchers with LocalSparkContext {

  val sc = getNewSpark("local[4]", "DDLTests")
  //implicit val ijs = TestSupportFcns.ijs
  val contextList = Array(
    ("SparkSQL",(iv: SparkContext) => new SQLContext(iv), TestSupportFcns.ijs), // test standard sparksql
    ("HiveQL",(iv: SparkContext) => new HiveContext(sc), TestSupportFcns.ijs) // text hiveql
  )
  for ((cmName,contextMapping, curIjs) <- contextList) {
    val sq = contextMapping(sc)

    test(s"$cmName : Listing all plugins") {
      import scOps._

      sq.registerImageJ(curIjs)

      val plugList = sq.sql("SELECT listplugins()")
      plugList.count shouldBe 1

      val outList = plugList.first().getList[String](0)
      outList.toArray.foreach(println(_))
      outList.size() should be > 100
    }


    test(cmName + ": Create Database Test") {
      sq.sql(s"""
                |CREATE TEMPORARY TABLE DebugImages
                |USING ch.fourquant.images.debug
                |OPTIONS (path "SimpleNeedednt Be Path-like", count "7", table "simple")
                        """.stripMargin.replaceAll("\n", " "))

      sq.sql("SHOW TABLES").collect().foreach(println(_))
      sq.tableNames().length shouldBe 1
      val tfi = sq.table("DebugImages")
      tfi.printSchema()

      tfi.schema(0).name shouldBe "sample"

      tfi.schema(1).name shouldBe "image"
      tfi.schema(1).dataType.typeName shouldBe "PortableImagePlusSQL"

      val oTab = sq.sql("SELECT sample,image FROM DebugImages")

      oTab.foreach(println(_))

      oTab.count shouldBe 7



    }

    test(cmName + ": Create Abstract Table") {
      sq.sql(s"""
                |CREATE TEMPORARY TABLE DebugImages
                |USING ch.fourquant.images.debug
                |OPTIONS (path "BigBadHaha/Must/Be/A/Path", count "7", table "abstract")
                        """.stripMargin.replaceAll("\n", " "))

      sq.sql("SHOW TABLES").collect().foreach(println(_))
      sq.tableNames().length shouldBe 1
      val tfi = sq.table("DebugImages")
      tfi.printSchema()

      tfi.schema(0).name shouldBe "path"
      tfi.schema(3).dataType.typeName shouldBe "array"
      tfi.schema(7).name shouldBe "image"

      val oTab = sq.sql("SELECT name,width,height,slices FROM DebugImages")
      oTab.collect.foreach(println(_))
      oTab.count shouldBe 7
    }

  }
}


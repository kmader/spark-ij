package ch.fourquant.images

import fourquant.imagej.TestSupportFcns
import org.apache.spark.{LocalSparkContext, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.scalatest.{FunSuite, Matchers}

/**
 * Created by mader on 10/8/15.
 */
class DDLTests extends FunSuite with Matchers with LocalSparkContext {

  val sc = getNewSpark("local[8]", "PureSQL")
  implicit val ijs = TestSupportFcns.ijs
  val contextList = Array(
    ("SparkSQL",(iv: SparkContext) => new SQLContext(iv)), // test standard sparksql
    ("HiveQL",(iv: SparkContext) => new HiveContext(sc)) // text hiveql
  )
  for ((cmName,contextMapping) <- contextList) {
    val sq = contextMapping(sc)


    test(cmName + ": Create Database Test") {
      sq.sql(s"""
                |CREATE TEMPORARY TABLE testFamilyImages
                |USING ch.fourquant.images.debug
                |OPTIONS (path "BigBadHaha")
                        """.stripMargin.replaceAll("\n", " "))

      sq.sql("SHOW TABLES").collect().foreach(println(_))
      sq.tableNames().length shouldBe 1
      val tfi = sq.table("testFamilyImages")
      tfi.printSchema()

      tfi.schema(0).name shouldBe "sample"
      tfi.schema(1).name shouldBe "image"

      val oTab = sq.sql("SELECT sample,image FROM testFamilyImages")
      oTab.count shouldBe 7

    }

  }
}


package com.romanidze.sparky.datamart.processing.weblogs

import java.net.{URI, URL}

import com.holdenkarau.spark.testing.{HDFSCluster, SharedSparkContext}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class WebLogsLoaderTest extends AnyFlatSpec with Matchers with SharedSparkContext {

  var hdfsCluster: HDFSCluster = null

  override def beforeAll(): Unit = {
    super.beforeAll()
    hdfsCluster = new HDFSCluster
    hdfsCluster.startHDFS()
  }

  it should "correctly load web logs by schema" in {

    val fs: FileSystem = FileSystem.get(new URI(hdfsCluster.getNameNodeURI()), new Configuration())
    val workingDir: Path = fs.getWorkingDirectory

    val testDir: Path = new Path(workingDir, "test")
    fs.mkdirs(testDir)

    val resourceURL: URL = getClass.getClassLoader.getResource("test.parquet")
    fs.copyFromLocalFile(new Path(resourceURL.toURI), new Path(testDir, "test.parquet"))

    implicit val spark: SparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()

    val logsLoader: WebLogsLoader = new WebLogsLoader()
    val logsDF: DataFrame = logsLoader.getWebLogsDataFrame(s"${workingDir}/test")

    //проверить визуально, что с схемой всё хорошо
    logsDF.printSchema()
    logsDF.show(1, 200, vertical = true)

    logsDF.count() > 0 shouldBe true

    spark.stop()

  }

  override def afterAll() {
    hdfsCluster.shutdownHDFS()
    super.afterAll()
  }

}

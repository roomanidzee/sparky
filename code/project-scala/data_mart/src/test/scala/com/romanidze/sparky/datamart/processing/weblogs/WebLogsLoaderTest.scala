package com.romanidze.sparky.datamart.processing.weblogs

import java.net.{URI, URL}

import com.holdenkarau.spark.testing.{HDFSCluster, SharedSparkContext}
import com.romanidze.sparky.datamart.processing.SchemaProvider
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class WebLogsLoaderTest extends AnyWordSpec with Matchers with SharedSparkContext {

  var hdfsCluster: HDFSCluster = null
  var workingDir: Path = null

  override def beforeAll(): Unit = {
    super.beforeAll()
    hdfsCluster = new HDFSCluster
    hdfsCluster.startHDFS()

    val fs: FileSystem = FileSystem.get(new URI(hdfsCluster.getNameNodeURI()), new Configuration())
    workingDir = fs.getWorkingDirectory

    val testDir: Path = new Path(workingDir, "test")
    fs.mkdirs(testDir)

    val resourceURL: URL = getClass.getClassLoader.getResource("test.parquet")
    fs.copyFromLocalFile(new Path(resourceURL.toURI), new Path(testDir, "test.parquet"))

  }

  "WebLogsLoader" should {

    "correctly load web logs by schema" in {

      implicit val spark: SparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()

      val logsLoader: WebLogsLoader = new WebLogsLoader()

      val logsDF: DataFrame = logsLoader.getWebLogsDataFrame(s"${workingDir}/test")

      logsDF.count() > 0 shouldBe true

      logsDF.schema shouldBe SchemaProvider.getWebLogsSchema

    }

    "expand dataframe by uid and url" in {

      implicit val spark: SparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()

      val logsLoader: WebLogsLoader = new WebLogsLoader()

      val logsDF: DataFrame = logsLoader.getWebLogsDataFrame(s"${workingDir}/test")
      val changedDF: DataFrame = logsLoader.processLogsDataFrame(logsDF)

      changedDF.count() > 0 shouldBe true

      val expectedSchema: StructType = StructType(
        Seq(
          StructField("uid", DataTypes.StringType),
          StructField("url", DataTypes.StringType),
          StructField("url_count", DataTypes.LongType, nullable = false)
        )
      )

      changedDF.schema shouldBe expectedSchema
      changedDF.show(20, 200, vertical = true)

    }

  }

  override def afterAll() {
    hdfsCluster.shutdownHDFS()
    super.afterAll()
  }

}

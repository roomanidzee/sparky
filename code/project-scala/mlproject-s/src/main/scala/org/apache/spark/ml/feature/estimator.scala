package org.apache.spark.ml.feature

import org.apache.hadoop.fs.Path
import org.apache.spark.ml.feature.SklearnEstimatorModel.SklearnEstimatorModelWriter
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.StructType

class SklearnEstimator(override val uid: String)
    extends Estimator[SklearnEstimatorModel]
    with DefaultParamsWritable {
  def this() = this(Identifiable.randomUID("SklearnEstimator"))

  override def fit(dataset: Dataset[_]): SklearnEstimatorModel = {
    ???
    // Внутри данного метода необходимо вызывать обучение модели при помощи train.py. Используйте для этого rdd.pipe().
    // Файл train.py будет возвращать сериализованную модель в формате base64.
    // Данный метод fit возвращает SklearnEstimatorModel, поэтому инициализируйте данный объект, где в качестве параметра будет приниматься модель в формате base64.
  }

  override def copy(extra: ParamMap): SklearnEstimator = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    // Определение выходной схемы данных
    ???
  }

}

object SklearnEstimator extends DefaultParamsReadable[SklearnEstimator] {
  override def load(path: String): SklearnEstimator = super.load(path)
}

class SklearnEstimatorModel(override val uid: String, val model: String)
    extends Model[SklearnEstimatorModel]
    with MLWritable {
  //как видно выше, для инициализации объекта данного класса в качестве одного из параметров конструктора является String-переменная model, это и есть модель в формате base64, которая была возвращена из train.py

  override def copy(extra: ParamMap): SklearnEstimatorModel = defaultCopy(extra)

  override def transform(dataset: Dataset[_]): DataFrame = {
    ???
    // Внутри данного метода необходимо вызывать test.py для получения предсказаний. Используйте для этого rdd.pipe().
    // Внутри test.py используется обученная модель, которая хранится в переменной `model`. Поэтому перед вызовом rdd.pipe() необходимо записать данное значение в файл и добавить его в spark-сессию при помощи sparkSession.sparkContext.addFile.
    // Данный метод возвращает DataFrame, поэтому полученные предсказания необходимо корректно преобразовать в DF.

  }

  override def transformSchema(schema: StructType): StructType = {
    ???
    // Определение выходной схемы данных
  }

  override def write: MLWriter = new SklearnEstimatorModelWriter(this)
}

object SklearnEstimatorModel extends MLReadable[SklearnEstimatorModel] {

  private[SklearnEstimatorModel] class SklearnEstimatorModelWriter(instance: SklearnEstimatorModel)
      extends MLWriter {

    private case class Data(model: String)

    override protected def saveImpl(path: String): Unit = {
      // В данном методе сохраняется значение модели в формате base64 на hdfs
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = Data(instance.model)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class SklearnEstimatorModelReader extends MLReader[SklearnEstimatorModel] {

    private val className = classOf[SklearnEstimatorModel].getName

    override def load(path: String): SklearnEstimatorModel = {
      // В данном методе считывается значение модели в формате base64 из hdfs
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read
        .parquet(dataPath)
        .select("model")
        .head()
      val modelStr = data.getAs[String](0)
      val model = new SklearnEstimatorModel(metadata.uid, modelStr)
      metadata.getAndSetParams(model)
      model
    }
  }

  override def read: MLReader[SklearnEstimatorModel] = new SklearnEstimatorModelReader

  override def load(path: String): SklearnEstimatorModel = super.load(path)
}

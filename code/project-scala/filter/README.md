# lab04a

## Сборка

```sbt package```

## Запуск

```spark-submit --conf spark.filter.topic_name=lab04_input_data --conf spark.filter.offset=earliest --conf spark.filter.output_dir_prefix=/user/name.surname/visits --class filter --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 .target/scala-2.11/filter_2.11-1.0.jar```
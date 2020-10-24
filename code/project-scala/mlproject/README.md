# mlproject

## Сборка
```
sbt package
```

## Запуск обучения
```
spark-submit --conf spark.mlproject.dataset_path=/labs/laba07/laba07.json \
             --conf spark.mlproject.model_save_path=/user/name.surname/spark-pipeline \
             --class train .target/scala-2.11/mlproject_2.11-1.0.jar 
```

## Запуск предсказания
```
spark-submit --conf spark.mlproject.model_save_path=/user/name.surname/spark-pipeline \
             --conf spark.mlproject.kafka_servers=spark-master-1:6667 \
             --conf spark.mlproject.input_topic=name_surname \
             --conf spark.mlproject.output_topic=name_surname_lab04b_out \
             --conf spark.mlproject.checkpoint_path=/user/name.surname/checkpoints \
             --class test \ 
             --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 \
             .target/scala-2.11/mlproject_2.11-1.0.jar 
```
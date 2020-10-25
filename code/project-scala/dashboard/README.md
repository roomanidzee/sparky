# dashboard

## Сборка
```
sbt assembly
```

## Запуск
```
spark-submit --conf spark.dashboard.dataset_path=/labs/laba08/laba08.json \
             --conf spark.dashboard.model_save_path=/user/name.surname/spark-pipeline \
             --conf spark.dashboard.es_address=10.0.0.5:9200 \
             --conf spark.dashboard.es_index=andrey_romanov_lab08 \
             --class dashboard .target/scala-2.11/dashboard_2.11-1.0.jar 
```
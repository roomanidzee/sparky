# data_mart

## Сборка
- ```sbt assembly```

## Деплой проекта

```
spark-submit --class data_mart .target/scala-2.11/data_mart_2.11-1.0.jar 
```
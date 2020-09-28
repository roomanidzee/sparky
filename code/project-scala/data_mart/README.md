# data_mart

## Требования перед работой с проектом

- Java 8 
- docker и docker-compose

## Запуск тестов

- ```docker-compose -f docker/docker-compose.yml up -d && docker logs -f es-indexer```
- ```sbt test```

## Сборка

- Положить в src/main/resources файл с конфигурациями. Пример есть в тестах
- ```docker-compose -f docker/docker-compose.yml up -d && docker logs -f es-indexer```
- ```sbt assembly```

## Деплой проекта

```
spark-submit --class data_mart --master yarn --deploy-mode cluster --driver-memory 4g --executor-memory 4g --conf spark.sql.shuffle.partitions=500 data_mart_2.11-1.0.jar
```
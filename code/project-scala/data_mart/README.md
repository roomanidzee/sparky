# data_mart

## Требования перед работой с проектом

- Java 8 
- docker и docker-compose

## Запуск тестов

- ```docker-compose -f docker/docker-compose/yml up -d```
- ```sbt test```

## Сборка

- ```docker-compose -f docker/docker-compose/yml up -d```
- ```sbt assembly```

## Деплой проекта

```
spark-submit --class com.romanidze.sparky.relevantsites.RelevantSitesApp --master yarn --deploy-mode client data_mart_2.11-1.0.jar *путь к файлу с конфигами*
```
# data_mart

## Требования перед работой с проектом

- Java 8 
- docker и docker-compose

## Запуск тестов

- ```docker-compose -f docker/docker-compose.yml up -d && docker logs -f es-indexer```
- ```sbt test```

## Сборка

- ```docker-compose -f docker/docker-compose.yml up -d && docker logs -f es-indexer```
- ```sbt assembly```

## Деплой проекта

```
spark-submit --class com.romanidze.sparky.relevantsites.RelevantSitesApp --master yarn --deploy-mode client data_mart_2.11-1.0.jar *config_file*
```

- config_file - путь к файлу с конфигурациями
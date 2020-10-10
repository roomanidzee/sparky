# users_items

## Сборка
```
sbt package
```

## Запуск
```
spark-submit --class users_items --conf spark.users_items.input_dir=/user/name.surname/visits --conf spark.users_items.output_dir=/user/name.surname/users-items --conf spark.users_items.update=0 .target/scala-2.11/users_items_2.11-1.0.jar 
```

Параметры:
 - ```spark.users_items.input_dir```  - папка для чтения данных в json - формате
 - ```spark.users_items.output_dir``` - папка для записи обработанных данных
 - ```spark.users_items.update```     - параметр для выбора режима работы
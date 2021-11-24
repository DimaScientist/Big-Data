# Лабораторная работа № 1: Введение в Apache Spark
***

## Цель работы:
* изучить операции загрузки и выгрузки данных в HDFS,
* ознакомиться с базовыми операциями Apache Spark в spark-shell,
* создать проект по обработке данных в IDE,
* отладить анализ данных велопарковок на локальном компьютере,
* запустить анализ данных велопарковок на сервере.


## Анализ данных велопарковок

1. Найти велосипед с максимальным пробегом.

Сначала создаются записи, где ключем выступает id велосипеда. 
Далее обрабатываем каждую запись о поездке по длительности и складываем их для всех записей, соответствующих ключу.
Прводим сортировку и выбираем первую запись. Печатаем id велосипеда.

```
    val tripsGroupedByBike = tripsInternal.keyBy(record => record.bikeId)
    val groupedByBikeSortByDuration = tripsGroupedByBike
      .mapValues(x => x.duration)
      .reduceByKey((trip1, trip2) => trip1 + trip2)

    val bikeWithBigDuration = groupedByBikeSortByDuration.map(row => row.swap).top(1)

    val bike_with_biggest_duration_id = bikeWithBigDuration.map(row => row._2).head
```
Результат:

![Велосипед с максимальным пробегом](https://github.com/DimaScientist/Big-Data/blob/main/LabWork1/images/task1.png)

2. Найти наибольшее расстояние между станциями.

Производим декартово произведение таблицы stationsInternal на саму себя при помощи ```cartesian```. Фильтруем пары так, чтобы в паре не было станций с одинаковыми названиями. Для каждой пары станции составляем кортеж: пара имен станций и расстояние, вычисленное по определенной [формуле](https://en.wikipedia.org/wiki/Haversine_formula). После сравниваем пары и выбираем с наибольшим расстоянием.

```
val r_earth = 6371

   val biggestDistance =  stationsInternal
     .cartesian(stationsInternal)
     .filter(stations => stations._1.name != stations._2.name)
     .map(row =>
       (
         (row._1.name, row._2.name),
         2 * r_earth * asin( sqrt( pow(sin((row._1.lat - row._2.lat) / 2),2)
           + (1 - pow(sin((row._1.lat - row._2.lat) / 2),2)
           - pow(sin((row._1.lat + row._2.lat) / 2),2) * pow(sin((row._1.long + row._2.long) / 2),2))))
       )
     )
     .reduce((first, second) => {if(first._2 > second._2) first else second})
```

Результат:

![Наибольшее расстояние между станциями](https://github.com/DimaScientist/Big-Data/blob/main/LabWork1/images/task2.png)

3. Найти путь велосипеда с максимальным пробегом через станции.

Сперва производится фильтрация записей, где отбрасываются все пути, которые не посетил велосипед с наибольшим пробегом.
Группируем записи по id велосипеда и сортируем их по дате отправления.

```
  val tripsByBikeWithBiggestDuration = tripsInternal
      .filter(trip => trip.bikeId == bike_with_biggest_duration_id)
      .groupBy(trip => trip.bikeId)
      .mapValues(x => x.toList.sortWith((trip1, trip2) => trip1.startDate.compareTo(trip2.startDate)< 0 ))
    tripsByBikeWithBiggestDuration.foreach(row => row._2.foreach(trip => println(trip.startStation, trip.endStation, trip.startDate, trip.endDate)))
```
Результат:

Первые несколько пунктов посещения:

![Путь велосипеда с максимальным пробегом через станции](https://github.com/DimaScientist/Big-Data/blob/main/LabWork1/images/task3.png)

4. Найти количество велосипедов в системе.

Количество велосипедов можно посчитать при помощи ```count()```. Также для отлова повторов надо поставить условие недопуска дубликатов ```distinct()```.

```
println(tripsInternal.map(trip => trip.bikeId).distinct().count())
```

Результат:

![Количество велосипедов в системе](https://github.com/DimaScientist/Big-Data/blob/main/LabWork1/images/task4.png)

5. Найти пользователей, потративших на поездки более 3 часов.

Производится фильтрация по длительности поездки среди велосипедов, у которых длительность пробега больше 3 часов.

```
tripsInternal
      .filter(trip => trip.duration > 3 * 60 * 60)
      .sortBy(trip => trip.duration)
      .map(trip => (trip.tripId, trip.bikeId))
      .take(10)
      .foreach(println)
```

Результат:

![Пользователи, потратившие на поездки более 3 часов](https://github.com/DimaScientist/Big-Data/blob/main/LabWork1/images/task5.png)

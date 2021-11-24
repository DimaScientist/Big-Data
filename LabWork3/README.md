# Лабораторная работа № 3: Потоковая обработка в Apache Flink
***

## Задание:
Выполнить следующие задания из набора заданий репозитория https://github.com/ververica/flink-training-exercises:

 * RideCleanisingExercise
 * RidesAndFaresExercise
 * HourlyTipsExerxise
 * ExpiringStateExercise


## Выполнение заданий:

### RideCleanisingExercise

__Задание__: *Офильтровать поездки, которые начались и закончились в Нью-Йорке. Вывести получившийся результат.*


__Решение__:

```
val filteredRides = rides.filter(ride =>
      // при помощи пакета утилит геолокации проверяем, находится ли конец и начало в Нью-Йорке
      // проверка идет по долготе и широте
      GeoUtils.isInNYC(ride.endLon, ride.endLat) && GeoUtils.isInNYC(ride.startLon, ride.startLat))
```

__Тест__:

![Тест RideCleanisingExercise](https://github.com/DimaScientist/Big-Data/blob/main/LabWork3/images/task_1_tests.png)


### RidesAndFaresExercise

__Задание__: *Обогатить поездки на такси информацией о плате.*


__Решение__:

```
  class EnrichmentFunction extends RichCoFlatMapFunction[TaxiRide, TaxiFare, (TaxiRide, TaxiFare)] {

    // текущее состояние потока поездки
    lazy val stateRide: ValueState[TaxiRide] = getRuntimeContext.getState(
      new ValueStateDescriptor[TaxiRide]("new ride", classOf[TaxiRide])
    )

    // текущее состояние потока платы за проезд
    lazy val stateFare: ValueState[TaxiFare] = getRuntimeContext.getState(
      new ValueStateDescriptor[TaxiFare]("new fare", classOf[TaxiFare])
    )

    override def flatMap1(ride: TaxiRide, out: Collector[(TaxiRide, TaxiFare)]): Unit = {
      // получаем текущее значение платы из потока
      val fare = stateFare.value()

      if(fare != null){
        // если значение не пустое, то текущее состояние очищается и добавляется пара: поездка-плата
        stateFare.clear()
        out.collect((ride, fare))
      }
      else{
        // иначе обновляем состояние поездки
        stateRide.update(ride)
      }
    }

    override def flatMap2(fare: TaxiFare, out: Collector[(TaxiRide, TaxiFare)]): Unit = {
      // получаем текущее значение поездки из потока
      val ride = stateRide.value()

      if(ride != null){
        // если значение не пустое, то текущее состояние очищается и добавляется пара: поездка-плата
        stateRide.clear()
        out.collect((ride, fare))
      }
      else{
        // иначе обновляем состояние платы
        stateFare.update(fare)
      }
    }
```

__Тест__:

![Тест RidesAndFaresExercise](https://github.com/DimaScientist/Big-Data/blob/main/LabWork3/images/task_2_tests.png)


### HourlyTipsExerxise

__Задание__: *Вычислить общее количество чаевых по каждому водителю каждый час, затем из полученного потока данных найти наибольшее колиество чаевых в каждом часе.*


__Решение__:

Основной блок решения:

```
// для начала найдем количество поездок ежечастно
val tipsPerHour = fares
      // для этого преобразуем данные в поток пар ID водителя и поездка
      .map(fare => (fare.driverId, fare.tip))
      // ключем будет ID водителя
      .keyBy(row => row._1)
      // создаем оконное представление для применение функции
      .timeWindow(Time.hours(1))
      // для поездок с одинаковыми ID водителя суммируем поездки
      .reduce(
        (f1: (Long, Float), f2: (Long, Float)) => {(f1._1, f1._2 + f2._2)},
        new ProcessingWithWindowInfo()
      )

// для нахождения наибольшей поездки каждый час
val maxTipEachHour = tipsPerHour
      .timeWindowAll(Time.hours(1))
      .maxBy(2)
```


Была написана вспомогательная оконная функция:

```
class ProcessingWithWindowInfo() extends ProcessWindowFunction[(Long, Float), (Long, Long, Float), Long, TimeWindow] {
    override def process(key: Long, context: Context, elements: Iterable[(Long, Float)], out: Collector[(Long, Long, Float)]): Unit = {
      // сохраняем итератор на следующую сумму
      val sum = elements.iterator.next()._2
      out.collect((context.window.getEnd, key, sum))
    }
  }
```

__Тест__:

![Тест HourlyTipsExerxise](https://github.com/DimaScientist/Big-Data/blob/main/LabWork3/images/task_3_tests.png)



### ExpiringStateExercise

__Задание__: *Обогатить поездки за такси информацией о плате за них.*


__Решение__:

```
 class EnrichmentFunction extends KeyedCoProcessFunction[Long, TaxiRide, TaxiFare, (TaxiRide, TaxiFare)] {

    // текущее состояние потока поездки
    lazy val stateRide: ValueState[TaxiRide] = getRuntimeContext.getState(
      new ValueStateDescriptor[TaxiRide]("new ride", classOf[TaxiRide])
    )

    // текущее состояние потока платы за проезд
    lazy val stateFare: ValueState[TaxiFare] = getRuntimeContext.getState(
      new ValueStateDescriptor[TaxiFare]("new fare", classOf[TaxiFare])
    )

    // функция вызова обработки для первого потока поездок из двух связанных
    override def processElement1(ride: TaxiRide,
                                 context: KeyedCoProcessFunction[Long, TaxiRide, TaxiFare, (TaxiRide, TaxiFare)]#Context,
                                 out: Collector[(TaxiRide, TaxiFare)]): Unit = {

      // получаем текущее значение платы из потока
      val fare = stateFare.value()

      if(fare != null){
        // если значение не пустое, то текущее состояние очищается и добавляется пара: поездка-плата
        stateFare.clear()
        // получаем результат
        out.collect((ride, fare))
        context.timerService().deleteEventTimeTimer(ride.getEventTime)
      }
      else{
        // иначе обновляем состояние поездки
        stateRide.update(ride)
        // запрашиваем метку времени и ожидаем до тех пор, пока не появится watermark
        context.timerService().registerEventTimeTimer(ride.getEventTime)
      }
    }

    // функция вызова обработки для второго потока платы из двух связанных
    override def processElement2(fare: TaxiFare,
                                 context: KeyedCoProcessFunction[Long, TaxiRide, TaxiFare, (TaxiRide, TaxiFare)]#Context,
                                 out: Collector[(TaxiRide, TaxiFare)]): Unit = {

      // получаем текущее значение поездки из потока
      val ride = stateRide.value()

      if(ride != null){
        // если значение не пустое, то текущее состояние очищается и добавляется пара: поездка-плата
        stateRide.clear()
        // получаем результат
        out.collect((ride, fare))
        context.timerService().deleteEventTimeTimer(ride.getEventTime)
      }
      else{
        // иначе обновляем состояние платы
        stateFare.update(fare)
        // запрашиваем метку времени и ожидаем до тех пор, пока не появится watermark
        context.timerService().registerEventTimeTimer(fare.getEventTime)
      }
    }

    // При вызове TimerService
    override def onTimer(timestamp: Long,
                         ctx: KeyedCoProcessFunction[Long, TaxiRide, TaxiFare, (TaxiRide, TaxiFare)]#OnTimerContext,
                         out: Collector[(TaxiRide, TaxiFare)]): Unit = {
      if(stateFare.value()!=null){
        // отправляем запись на выход по тегу
        ctx.output(unmatchedFares, stateFare.value())
        stateFare.clear()
      }
      if(stateRide.value()!=null){
        ctx.output(unmatchedRides, stateRide.value())
        stateRide.clear()
      }
    }

```

__Тест__:

![Тест ExpiringStateExercise](https://github.com/DimaScientist/Big-Data/blob/main/LabWork3/images/task_4_tests.png)

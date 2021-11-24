/*
 * Copyright 2017 data Artisans GmbH, 2019 Ververica GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.flinktraining.exercises.datastream_scala.process

import com.ververica.flinktraining.exercises.datastream_java.datatypes.{TaxiFare, TaxiRide}
import com.ververica.flinktraining.exercises.datastream_java.sources.{TaxiFareSource, TaxiRideSource}
import com.ververica.flinktraining.exercises.datastream_java.utils.{ExerciseBase, MissingSolutionException}
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase._
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector

/**
  * The "Expiring State" exercise of the Flink training
  * (http://training.ververica.com).
  *
  * The goal for this exercise is to enrich TaxiRides with fare information.
  *
  * Parameters:
  * -rides path-to-input-file
  * -fares path-to-input-file
  *
  */
object ExpiringStateExercise {
  val unmatchedRides = new OutputTag[TaxiRide]("unmatchedRides") {}
  val unmatchedFares = new OutputTag[TaxiFare]("unmatchedFares") {}

  def main(args: Array[String]) {

    // parse parameters
    val params = ParameterTool.fromArgs(args)
    val ridesFile = params.get("rides", ExerciseBase.pathToRideData)
    val faresFile = params.get("fares", ExerciseBase.pathToFareData)

    val maxDelay = 60            // events are out of order by max 60 seconds
    val servingSpeedFactor = 600 // 10 minutes worth of events are served every second

    // set up streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(ExerciseBase.parallelism)

    val rides = env
      .addSource(rideSourceOrTest(new TaxiRideSource(ridesFile, maxDelay, servingSpeedFactor)))
      .filter { ride => ride.isStart && (ride.rideId % 1000 != 0) }
      .keyBy(_.rideId)

    val fares = env
      .addSource(fareSourceOrTest(new TaxiFareSource(faresFile, maxDelay, servingSpeedFactor)))
      .keyBy(_.rideId)

    val processed = rides.connect(fares).process(new EnrichmentFunction)

    printOrTest(processed.getSideOutput[TaxiFare](unmatchedFares))

    env.execute("ExpiringState (scala)")
  }

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
  }

}

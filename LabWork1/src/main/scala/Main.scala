package org.example

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import java.io.File
import java.time.format.DateTimeFormatter
import org.apache.log4j.{Level, Logger}
import models._

import java.time.LocalDate


object Main {

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.spark-project").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {

    val appName = "LabWork1"
    val master = "local[2]"


    val Seq(masterURL, tripDataPath, stationDataPath) = args.toSeq
//    val config = new SparkConf().setAppName(appName).setMaster(master)
    val config = new SparkConf().setAppName(appName).setMaster(masterURL)

    val sc = new SparkContext(config)

    val tripData = sc.textFile(tripDataPath)
    val stationData = sc.textFile(stationDataPath)

//    val tripData = sc.textFile((new File("./src/resources/trips.csv").getPath))
//    val stationData = sc.textFile((new File("./src/resources/stations.csv")).getPath)

    val stationHeader = stationData.first()
    val stations = stationData.filter(row=>row!=stationHeader)


    val tripHeader = tripData.first()
    val trips = tripData.filter(row=>row!=tripHeader).map(row=>row.split(",", -1))

    println("\nЗаголовки stations")
    stationHeader.foreach(print)

    println("\nЗаголовки trips")
    tripHeader.foreach(print)

    val stationIndexed = stations.keyBy(row=>row(0).toInt)

    val tripsByStartTerminals = trips.keyBy(row=>row(0).toInt)
    val tripsByEndTerminal = trips.keyBy(row=>row(1).toInt)

    val startTrips = stationIndexed.join(tripsByStartTerminals)
    val endTrips = stationIndexed.join(tripsByEndTerminal)

    println("\nstartTrips")
    print(startTrips.toDebugString)
    println("\n count:" + startTrips.count())

    println("\nendTrips")
    print(endTrips.toDebugString)
    println("\n count: " + endTrips.count())

    val tripsInternal = trips.mapPartitions(rows=> {
      val timeFormat = DateTimeFormatter.ofPattern("yyyy-M-d H:m")
      rows.map(row=> Trip(
        tripId = row(0).toInt,
        duration = row(1).toInt,
        startDate = LocalDate.parse(row(2), timeFormat),
        startStation = row(3).toString,
        startTerminal = row(4).toInt,
        endDate = LocalDate.parse(row(5), timeFormat),
        endStation = row(6).toString,
        endTerminal = row(7).toInt,
        bikeId = row(8).toInt,
        subscriptionType = row(9).toString,
        zipCode = row(10).toString
      ))
    })

    print("\n" + tripsInternal.first + "\n")
    print("\n" + tripsInternal.first.startDate + "\n")

    val stationsInternal = stations.map(row=>Station(
      stationId = row(0).toInt,
      name = row(1).toString,
      lat = row(2).toDouble,
      long = row(3).toDouble,
      dockcount = row(4).toInt,
      landmark = row(5).toString,
      installation = row(6).toString,
      notes = null
    ))

    print("\n" + stationsInternal.first + "\n")
    print("\n" + stationsInternal.first.landmark + "\n")

    println("------------------------------------------------------")
    println("Задача 1: Найти велосипед с максимальным пробегом.")
    println("------------------------------------------------------")

    val tripsGroupedByBike = tripsInternal.keyBy(record => record.bikeId)
    val groupedByBikeSortByDuration = tripsGroupedByBike
      .mapValues(x => x.duration)
      .reduceByKey((trip1, trip2) => trip1 + trip2)

    val bikeWithBigDuration = groupedByBikeSortByDuration.map(row => row.swap).top(1)

    val bike_with_biggest_duration_id = bikeWithBigDuration.map(row => row._2).head

    println(bike_with_biggest_duration_id)



    println("------------------------------------------------------")

    println("------------------------------------------------------")
    println("Задача 2: Найти наибольшее расстояние между станциями.")
    println("------------------------------------------------------")

    val stationsByDuration = tripsInternal
      .filter(trip => trip.startStation != trip.endStation)
      .keyBy(trip => (trip.startStation, trip.endStation))
      .mapValues(trip => trip.duration)

    val durationsAroundStation = stationsByDuration.
      aggregateByKey((0, 0))(
        (acc, value) => (acc._1 + value, acc._2 + 1),
        (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)).mapValues(acc => acc._1 / acc._2)

    val stationsWithBiggestDuration = durationsAroundStation.map(row => row.swap).top(1)
    stationsWithBiggestDuration.foreach(println)

    println("------------------------------------------------------")

    println("------------------------------------------------------")
    println("Задача 3: Найти путь велосипеда с максимальным пробегом через станции.")
    println("------------------------------------------------------")

    val tripsByBikeWithBiggestDuration = tripsInternal
      .filter(trip => trip.bikeId == bike_with_biggest_duration_id)
      .groupBy(trip => trip.bikeId)
      .mapValues(x => x.toList.sortWith((trip1, trip2) => trip1.startDate.compareTo(trip2.startDate)< 0 ))
    tripsByBikeWithBiggestDuration.foreach(row => row._2.foreach(trip => println(trip.startStation, trip.endStation, trip.startDate, trip.endDate)))

    println("------------------------------------------------------")

    println("------------------------------------------------------")
    println("Задача 4: Найти количество велосипедов в системе.")
    println("------------------------------------------------------")

    println(tripsInternal.map(trip => trip.bikeId).distinct().count())

    println("------------------------------------------------------")


    println("------------------------------------------------------")
    println("Задача 5: Найти пользователей потративших на поездки более 3 часов.")
    println("------------------------------------------------------")

    tripsInternal
      .filter(trip => trip.duration > 3 * 60 * 60)
      .sortBy(trip => trip.duration)
      .map(trip => (trip.tripId, trip.bikeId))
      .take(10)
      .foreach(println)

    println("------------------------------------------------------")


    sc.stop()
  }

}

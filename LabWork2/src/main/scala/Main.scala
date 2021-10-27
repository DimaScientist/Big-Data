package org.example

import breeze.linalg.split
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import java.time.LocalDate
import java.time.format.DateTimeFormatter


object Main {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project").setLevel(Level.WARN)
    Logger.getLogger("org.apache.parquet").setLevel(Level.WARN)

    val appName = "LabWork1"
    val master = "local[2]"

    val config = new SparkConf().setAppName(appName).setMaster(master)
    val sc = new SparkContext(config)

    val spark = SparkSession.builder().appName(appName).config(config).getOrCreate()


    val programLanguagesPath = "./src/resources/programming-languages.csv"
    val dfProgramLanguages = spark.read.option("header", true).csv(programLanguagesPath)
    println("Выборка данных о языках программирования: ")
    dfProgramLanguages.show(5)

    val postsPath = "F:/posts_sample.xml"

    val dfPosts = spark.read.format("com.databricks.spark.xml").option("rowTag", "row").load(postsPath)
    println("Выборка данных о постах: ")
    dfPosts.show(5)

    println("Типы данных в DataFrame, содержащий информацию о постах:")
    dfPosts.printSchema()

    import spark.implicits._

    val startYear = 2010
    val endYear = 2020

    dfPosts
      .filter(s"_Tags IS NOT NULL AND _CreationDate BETWEEN '${startYear}' AND '${endYear}'")
      .show()


    sc.stop()
  }
}

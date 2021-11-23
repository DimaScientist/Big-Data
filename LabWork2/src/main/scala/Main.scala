package org.example

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lower


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


    val programLanguagesPath = "/media/dmitry/Dima/programming-languages.csv"

    var dfProgramLanguages = spark.read.option("header", value = true)
      .csv(programLanguagesPath)
    dfProgramLanguages = dfProgramLanguages.withColumn("name", lower(dfProgramLanguages("name")))

    println("Выборка данных о языках программирования: ")
    dfProgramLanguages.show(10)

    val postsPath = "/media/dmitry/Dima/posts_sample.xml"

    val dfPosts = spark.read.format("com.databricks.spark.xml").option("rowTag", "row").load(postsPath)
    println("Выборка данных о постах: ")
    dfPosts.show(10)

    println("Типы данных в DataFrame, содержащий информацию о постах:")
    dfPosts.printSchema()

    import spark.implicits._

    val startYear = 2010
    val endYear = 2021


    dfPosts.createOrReplaceTempView("posts")
    dfProgramLanguages.createOrReplaceTempView("languages")


    val tagsBetweenYears = spark.sql(s"" +
      s"SELECT " +
      s"_Id AS id, " +
      s"_Tags AS tags " +
      s"FROM posts " +
      s"WHERE _CreationDate BETWEEN '${startYear}' " +
      s"AND '${endYear}' AND _Tags IS NOT NULL;")
      .map(
        row => (
          row.getLong(0),
          row.getString(1)
            .replace("><", " ")
            .replace(">", "")
            .replace("<", "")
        )
      )
      .rdd
      .flatMap(row => row._2.split(" "))
      .map(tag => (tag, 1))
      .reduceByKey(_ + _)
      .toDF("tag", "count_posts")

    tagsBetweenYears.show()

    tagsBetweenYears.createOrReplaceTempView("tags")

    val languagesDF = spark.sql("" +
      "SELECT " +
      "tags.tag AS language, " +
      "tags.count_posts AS count_posts " +
      "FROM tags " +
      "WHERE EXISTS (SELECT 1 FROM languages WHERE languages.name LIKE '%' || tags.tag || '%')" +
      "SORT BY tags.count_posts DESC;"
    )

    languagesDF.write.parquet("languages.parquet")

    spark.read.parquet("languages.parquet").show(10)


    sc.stop()
  }
}

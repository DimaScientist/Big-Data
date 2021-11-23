# Лабораторная работа № 2: Формирование отчётов в Apache Spark
***

## Задание:
Сформировать отчёт с информацией о 10 наиболее популярных языках программирования по итогам года за период с 2010 по 2020 годы. Получившийся отчёт сохранить в формате Apache Parquet.
Для выполнения задания вы можете использовать любую комбинацию Spark API: _RDD API, Dataset API, SQL API_.

## Анализ данных о языках программирования

Для начала загрузим данные из файла в DataFrame:

```
// данные о языках программирования поставляются с заголовком
var dfProgramLanguages = spark.read.option("header", value = true)
    .csv(programLanguagesPath)
// приведем данные в колонке name к общему нижнему регистру
dfProgramLanguages = dfProgramLanguages.withColumn("name", lower(dfProgramLanguages("name")))
...
// для загрузки данных из xml был использован сторонний формат com.databricks.spark.xml
val dfPosts = spark.read.format("com.databricks.spark.xml").option("rowTag", "row").load(postsPath)

```

Выборка о языках программирования получилась следующей:

![Выборка о языках программирования](https://github.com/DimaScientist/Big-Data/blob/main/LabWork2/images/languages_df.png)

Схема DataFrame о постах получилась следующей: 

![Схема DataFrame о постах](https://github.com/DimaScientist/Big-Data/blob/main/LabWork2/images/posts_scheme.png)

Создадим для этих двух таблиц временное представления для дальнейшей работы с ними в качестве SQL-таблиц:

```
dfPosts.createOrReplaceTempView("posts")
dfProgramLanguages.createOrReplaceTempView("languages")
```

Для начала найдем количество всех тегов, которые находятся в постах за заданный период времени:

```
val tagsBetweenYears = spark
      // произведем выборку тегов и ID постов, к которым эти теги относятся
      .sql(s"SELECT " +
      s"_Id AS id, " +
      s"_Tags AS tags " +
      s"FROM posts " +
      s"WHERE _CreationDate BETWEEN '${startYear}' " +
      s"AND '${endYear}' AND _Tags IS NOT NULL;")
      // так как теги представлены в виде xml-разметки, избавимся от ненужных знаков
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
      // каждый тег и ID поста вынесем в отдельную строку
      .flatMap(row => row._2.split(" "))
      // просуммируем найдем количество постов для каждого тега
      .map(tag => (tag, 1))
      .reduceByKey(_ + _)
      .toDF("tag", "count_posts")
```

Получившуюся таблицу также преобразуем во временное представление:

```
tagsBetweenYears.createOrReplaceTempView("tags")
```

После при помощи SQL отберем только те теги, которые есть в DataFrame со списком языков программирования, и отсортируем его по количеству постов:

```
val languagesDF = spark.sql("" +
      "SELECT " +
      "tags.tag AS language, " +
      "tags.count_posts AS count_posts " +
      "FROM tags " +
      "WHERE EXISTS (SELECT 1 FROM languages WHERE languages.name LIKE '%' || tags.tag || '%')" +
      "SORT BY tags.count_posts DESC;"
    )
```

Сохраним его в parquet и выведем первые 10 языков программирования:

```
languagesDF.write.parquet("languages.parquet")

spark.read.parquet("languages.parquet").show(10)
```

Получился следующий результат:

![Результат](https://github.com/DimaScientist/Big-Data/blob/main/LabWork2/images/results.png)



import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions.{col, monotonically_increasing_id}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

object Main extends App {
  lazy val currentDir = System.getProperty("user.dir") + "/src/main/scala/"
  //  Создадим SparkSession
  lazy val spark = SparkSession.builder()
    .config("spark.master", "local")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
  // Отключаем лишние логи
  spark.sparkContext.setLogLevel("WARN")

  //  Создадим струкруру будущего DataFrame-а в соотвествии со схемой:
  //  user id | item id | rating | timestamp
  lazy val structRating = StructType(Array(
    StructField("user_id", IntegerType, false),
    StructField("item_id", IntegerType, false),
    StructField("rating", IntegerType, false),
    StructField("timestamp", IntegerType, false),
  ))

  // Загрузим файл с данными в формате CSV
  lazy val df = spark.read
    .option("delimiter", "\\t")
    .option("header", "false")
    .schema(structRating)
    .csv(currentDir + "u.data")
  println("Таблица 'рейтинг фильмов'")
  df.show(5)


  //  В поле “hist_film” нужно указать для заданного id фильма
  //  количество поставленных оценок в следующем порядке: "1", "2", "3", "4", "5".
  //  То есть сколько было единичек, двоек, троек и т.д.

  var df_film = df.select("item_id", "rating")
    .where("item_id == 1126")
    .groupBy("rating")
    .count()
    .sort("rating")
    .select(col("count").alias("hist_film"))

  println("Кол-во поставленных оценок для фильма с id=1126")
  println("в следующем порядке: \"1\", \"2\", \"3\", \"4\", \"5\"")
  df_film.show()


  //  В поле “hist_all” нужно указать то же самое только для всех фильмов
  //  общее количество поставленных оценок в том же порядке: "1", "2", "3", "4", "5".
  var df_all = df.select("item_id", "rating")
    .groupBy("rating")
    .count()
    .sort("rating")
    .select(col("count").alias("hist_all"))

  println("Кол-во поставленных оценок для всех фильмов")
  println("в следующем порядке: \"1\", \"2\", \"3\", \"4\", \"5\"")
  df_all.show()

  df_film = df_film.withColumn("id", monotonically_increasing_id())
  df_all = df_all.withColumn("id", monotonically_increasing_id())
  lazy val df_out = df_film
    .join(df_all, df_film("id") === df_all("id"), "outer")
    .drop("id")

  println("Объединим полученные данные в одну табл.")
  df_out.show()

  println("Сохраним полученные данные в файл df_out.json")
  val jsonDs = df_out.toJSON
  val count = jsonDs.count()
  jsonDs
    .repartition(1)
    .rdd
    .zipWithIndex()
    .map { case (json, idx) =>
      if (idx == 0) "[\n" + json + "," // first row
      else if (idx == count - 1) json + "\n]" // last row
      else json + ","
    }
    .saveAsTextFile(currentDir + "df_out.json")
}

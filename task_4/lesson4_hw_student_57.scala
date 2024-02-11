package hw_4

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{broadcast, col, count, sum}

object hw_4 extends App {
  val spark = SparkSession
    .builder()
    .master("local[1]")
    .appName("hw_4")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
  spark.conf.set("spark.sql.adaptive.enabled", value = false)

  // Задание 1.

  val df = spark.read.option("header", value = true).csv("src/main/resources/hw_4/country_info.csv")
    .groupBy("country")
    .agg(
      count("*"),
      functions.max("name")
    ).repartition(3)

  df.explain()
  df.show()
  System.in.read()

  // Задание 2.

  //  1. План считывает файл Artists. Используются колонки Name, ID, Followers. Колонка Followers, ID фильтруется по
  //     фильтру Isnotnull. Столбец Followers переводится в int и на него применяется фильтр > 50000000
  //  2. Artists сортируется по полю ID
  //  3. План считывает файл Top_Songs_US. Используются колонки Artist ID, Song Duration.
  //     Колонка Artist ID фильтруется по фильтру Isnotnull
  //  4. Top_Songs_US сортируется по полю Artist ID
  //  5. Выполняется Inner Join по колонкам "ID" датафрейма dfArtists и колонке "Artist ID" датафрейма dfSongs.
  //  6. Происходит аггрегация данных на партициях для подсчета суммарной длительности песен каждого из артистов.
  //     Дальше происходит финальная агрегация. То есть объединение всех агрегаций.

  // ШАГ С SortMergeJoin ЗАНИМАЕТ 335 ms

  //  *(6) HashAggregate(keys=[Name#17], functions=[sum(cast(Song Duration#63 as double))])
  //  +- Exchange hashpartitioning(Name#17, 200), ENSURE_REQUIREMENTS, [plan_id=215]
  //  +- *(5) HashAggregate(keys=[Name#17], functions=[partial_sum(cast(Song Duration#63 as double))])
  //  +- *(5) Project [Song Duration#63, Name#17]
  //  +- *(5) SortMergeJoin [Artist ID#53], [ID#18], Inner
  //  :- *(2) Sort [Artist ID#53 ASC NULLS FIRST], false, 0
  //    :  +- Exchange hashpartitioning(Artist ID#53, 200), ENSURE_REQUIREMENTS, [plan_id=197]
  //  :     +- *(1) Filter isnotnull(Artist ID#53)
  //  :        +- FileScan csv [Artist ID#53,Song Duration#63] Batched: false, DataFilters: [isnotnull(Artist ID#53)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/aleksei.kulikov@corp.deecoob.com/repos/private/scala/semina..., PartitionFilters: [], PushedFilters: [IsNotNull(Artist ID)], ReadSchema: struct<Artist ID:string,Song Duration:string>
  //    +- *(4) Sort [ID#18 ASC NULLS FIRST], false, 0
  //  +- Exchange hashpartitioning(ID#18, 200), ENSURE_REQUIREMENTS, [plan_id=206]
  //  +- *(3) Project [Name#17, ID#18]
  //  +- *(3) Filter ((isnotnull(Followers#24) AND (cast(Followers#24 as int) > 50000000)) AND isnotnull(ID#18))
  //  +- FileScan csv [Name#17,ID#18,Followers#24] Batched: false, DataFilters: [isnotnull(Followers#24), (cast(Followers#24 as int) > 50000000), isnotnull(ID#18)], Format: CSV, Locat


  val dfArtists = spark.read.option("header", value = true).csv("src/main/resources/hw_4/Artists.csv")
  val dfSongs = spark.read.option("header", value = true).csv("src/main/resources/hw_4/Top_Songs_US.csv")

  val joinedDf = dfSongs.as("df1")
    .join(dfArtists.as("df2"), col("df1.Artist ID") === col("df2.ID"))
    .filter(col("Followers") > 50000000)
    .groupBy("Name")
    .agg(
      sum("Song Duration").as("Total Duration")
    )

  joinedDf.show()
  joinedDf.explain()
  System.in.read()

  // Задание 3.

  //  1. План считывает файл Artists. Используются колонки Name, ID, Followers. Колонка Followers,  ID фильтруется по
  //     фильтру Isnotnull. Столбец Followers переводится в int и на него применяется фильтр > 50000000
  //     Затем идет парционирование и бродкастинг на ноды.
  //  2. План считывает файла Top_Songs_US. Используются колонки Artist ID, Song Duration.
  //     Колонка Artist ID фильтруется по фильтру Isnotnull.
  //  3. Происходит BroadcastHashJoin с таблицей Artists.
  //  4. Происходит аггрегация данных на партициях для подсчета суммарной длительности песен каждого из артистов.
  //     Дальше происходит финальная агрегация. То есть объединение всех агрегаций.

  // ШАГ С BroadcastHashJoin ЗАНИМАЕТ 139 ms

  //  *(3) HashAggregate(keys=[Name#17], functions=[sum(cast(Song Duration#63 as double))])
  //  +- Exchange hashpartitioning(Name#17, 200), ENSURE_REQUIREMENTS, [plan_id=176]
  //  +- *(2) HashAggregate(keys=[Name#17], functions=[partial_sum(cast(Song Duration#63 as double))])
  //  +- *(2) Project [Song Duration#63, Name#17]
  //  +- *(2) BroadcastHashJoin [Artist ID#53], [ID#18], Inner, BuildRight, false
  //  :- *(2) Filter isnotnull(Artist ID#53)
  //  :  +- FileScan csv [Artist ID#53,Song Duration#63] Batched: false, DataFilters: [isnotnull(Artist ID#53)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/aleksei.kulikov.., PartitionFilters: [], PushedFilters: [IsNotNull(Artist ID)], ReadSchema: struct<Artist ID:string,Song Duration:string>
  //    +- BroadcastExchange HashedRelationBroadcastMode(List(input[1, string, true]),false), [plan_id=170]
  //  +- *(1) Project [Name#17, ID#18]
  //  +- *(1) Filter ((isnotnull(Followers#24) AND (cast(Followers#24 as int) > 50000000)) AND isnotnull(ID#18))
  //  - FileScan csv [Name#17,ID#18,Followers#24] Batched: false, DataFilters: [isnotnull(Followers#24), (cast(Followers#24 as int) > 50000000), isnotnull(ID#18)], Format: CSV, Location: InMemoryFileInde

  val broadDf = dfSongs.as("df1")
    .join(broadcast(dfArtists).as("df2"), col("df1.Artist ID") === col("df2.ID"))
    .filter(col("Followers") > 50000000)
    .groupBy("Name")
    .agg(
      sum("Song Duration").as("Total Duration")
    )

  broadDf.show()
  broadDf.explain()
  System.in.read()

}
package homework

import org.apache.spark.sql.{SaveMode, SparkSession}

object CreateWideTable {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()

    val personDf = spark.sql("select * from student57.person")
    val cardDf = spark.sql("select * from student57.card")
    val addressDf = spark.sql("select * from student57.person_adress")
    val wideDf = personDf.join(cardDf, Seq("guid")).join(addressDf, Seq("guid"))

    wideDf.repartition(9).write.mode(SaveMode.Overwrite).saveAsTable("student57.wide_table")

    spark.close()
  }

}

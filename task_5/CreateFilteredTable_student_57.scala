package homework

import org.apache.spark.sql.{SaveMode, SparkSession}

object CreateFilteredTable {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()

    val wideDf = spark.sql("select * from student57.wide_table")

    val filteredResult = wideDf.filter(
      wideDf("gender") === "Female" &&
        wideDf("age") > 50 &&
        wideDf("amount") > 3000 &&
        wideDf("province") === "QC" &&
        wideDf("zip").startsWith("8")
    )

    filteredResult.write.mode(SaveMode.Overwrite).saveAsTable("student57.filteredResult")

    spark.close()
  }
}
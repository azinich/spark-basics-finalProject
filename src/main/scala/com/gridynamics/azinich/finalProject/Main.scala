package com.gridynamics.azinich.finalProject

import java.nio.file.Paths

import org.apache.log4j.{Level, Logger}

object Main {
  import org.apache.spark.sql.SparkSession

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Final Project")
      .config("spark.master", "local")
      .getOrCreate()


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val inputWarehouse = Paths.get(getClass.getResource("/input_warehouses.csv").toURI).toString
    val inputAmounts = Paths.get(getClass.getResource("/input_amounts.csv").toURI).toString
    val keeper = new Keeper(inputWarehouse, inputAmounts)
    keeper.showStatistics(spark)
  }
}

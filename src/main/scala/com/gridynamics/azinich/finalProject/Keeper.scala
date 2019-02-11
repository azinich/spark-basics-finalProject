package com.gridynamics.azinich.finalProject

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._


class Keeper(warehouseInputPath: String, amountsInputPath: String) {
  private val DecimalType: DecimalType = DataTypes.createDecimalType(15, 5)
  val warehouseSchema: StructType = new StructType(Array(
    StructField("positionId", LongType, nullable = false),
    StructField("warehouse", StringType, nullable = false),
    StructField("product", StringType, nullable = false),
    StructField("eventTime", LongType, nullable = false)
  ))

  val amountsSchema: StructType = new StructType(Array(
    StructField("positionId", LongType, nullable = false),
    StructField("amount", DecimalType, nullable = false),
    StructField("eventTime", LongType, nullable = false)
  ))


  def showStatistics(ss: SparkSession): Unit = {
    val warehouses: DataFrame = getWarehouses(ss)
    val amounts: DataFrame = getAmounts(ss)

    val currentAmounts: DataFrame = getCurrentAmounts(amounts)

    val currentAmountsForEachWHandP: DataFrame = getCurrentAmountsForEachWarehouseAndPosition(warehouses, currentAmounts)
    currentAmountsForEachWHandP.show()

    val minimalsForEachWHandP: DataFrame = getMin(warehouses, currentAmounts)
    val maximalsForEachWHandP: DataFrame = getMax(warehouses, currentAmounts)
    val averageForEachWHandP: DataFrame = getAvg(warehouses, currentAmounts)

    minimalsForEachWHandP.show()
    maximalsForEachWHandP.show()
    averageForEachWHandP.show()

  }

  def getMin(warehouses: DataFrame, amounts: DataFrame): DataFrame = {
    joinedAndGrouped(warehouses, amounts).min("amount")
  }

  def getMax(warehouses: DataFrame, amounts: DataFrame): DataFrame = {
    joinedAndGrouped(warehouses, amounts).max("amount")
  }

  def getAvg(warehouses: DataFrame, amounts: DataFrame): DataFrame = {
    joinedAndGrouped(warehouses, amounts).avg("amount").cache()
  }

  private def joinedAndGrouped(warehouses: DataFrame, amounts: DataFrame) = {
    warehouses.join(amounts, "positionId").groupBy("warehouse", "product")
  }

  def getCurrentAmountsForEachWarehouseAndPosition(warehouses: DataFrame, currentAmounts: DataFrame): DataFrame = {
    warehouses.join(currentAmounts, "positionId")
      .select("positionId", "warehouse", "product", "amount")
  }

  def getCurrentAmounts(amounts: DataFrame): DataFrame = {
    amounts.groupBy("positionId").agg(max("eventTime").as("eventTime"))
      .join(amounts, List("positionId", "eventTime"))
      .select("positionId", "amount", "eventTime")
  }


  def getWarehouses(ss: SparkSession): DataFrame = {
    parseFile(ss, warehouseInputPath, warehouseSchema)
  }

  def getAmounts(ss: SparkSession): DataFrame = {
    parseFile(ss, amountsInputPath, amountsSchema)
  }

  private def parseCSV(ss: SparkSession, inputPath: String, schema: StructType): DataFrame = {
    ss.read.schema(schema).option("header", "true").csv(inputPath)
  }

  private def parseJSON(ss: SparkSession, inputPath: String, schema: StructType): DataFrame = {
    ss.read.schema(schema).json(inputPath)
  }

  def parseFile(ss: SparkSession, inputPath: String, schema: StructType): DataFrame = {
    inputPath.split("\\.").last match {
      case "csv" => parseCSV(ss: SparkSession, inputPath, schema)
      case "json" => parseJSON(ss: SparkSession, inputPath, schema)
      case _ => throw new FileTypeNotSupportedException()
    }
  }
}

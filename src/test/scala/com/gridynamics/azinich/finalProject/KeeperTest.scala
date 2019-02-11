package com.gridynamics.azinich.finalProject

import java.math
import java.nio.file.Paths

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite, TestData}
import org.apache.spark.sql.functions._

@RunWith(classOf[JUnitRunner])
class KeeperTest extends FunSuite with BeforeAndAfterEach with BeforeAndAfterAll {

  private var ss: SparkSession = _
  private var inputWarehouse: String = _
  private var inputAmounts: String = _
  private var testPath: String = _
  private var testSchema: StructType = _

  override protected def beforeAll(): Unit = {
    initSS()
    inputWarehouse = Paths.get(getClass.getResource("/input_warehouses.csv").toURI).toString
    inputAmounts = Paths.get(getClass.getResource("/input_amounts.csv").toURI).toString
    testPath = Paths.get(getClass.getResource("/testFile.csv").toURI).toString
    testSchema = StructType(List(
      StructField("col1", IntegerType),
      StructField("col2", IntegerType)
    ))
  }

  private def initSS(): Unit = {
    ss = SparkSession
      .builder()
      .appName("Final Project")
      .config("spark.master", "local")
      .getOrCreate()
  }

  override protected def beforeEach(): Unit = {
    if(ss == null) {
      initSS()
    }
  }

  override protected def afterAll(): Unit = {
    ss.close()
    ss = null
  }

  test("testGetCurrentAmountsForEachWarehouseAndPosition") {
    val keeper = new Keeper(inputWarehouse, inputAmounts)

    val amounts = keeper.getAmounts(ss)
    val warehouses = keeper.getWarehouses(ss)

    val curAmounts = keeper.getCurrentAmountsForEachWarehouseAndPosition(warehouses, amounts)
    val count = curAmounts.count()

    assert(count === 9)
  }

  test("testGetAvg") {
    val keeper = new Keeper(inputWarehouse, inputAmounts)
    val rightAmount = math.BigDecimal.valueOf(9.0)

    val amounts = keeper.getCurrentAmounts(keeper.getAmounts(ss))
    val warehouses = keeper.getWarehouses(ss)

    val avgs = keeper.getAvg(warehouses, amounts)
      .filter(col("warehouse").===("W-1") && col("product").===("P-1")).collect().head
    val amount = avgs.get(2).asInstanceOf[math.BigDecimal]

    assert(amount.compareTo(rightAmount) == 0)
  }

  test("testGetMin") {

    val keeper = new Keeper(inputWarehouse, inputAmounts)
    val rightAmount = math.BigDecimal.valueOf(8.0)

    val amounts = keeper.getCurrentAmounts(keeper.getAmounts(ss))
    val warehouses = keeper.getWarehouses(ss)

    val min: Row = keeper.getMin(warehouses, amounts)
      .filter(col("warehouse").===("W-1") && col("product").===("P-1")).collect().head
    val amount = min.get(2).asInstanceOf[math.BigDecimal]

    assert(amount.compareTo(rightAmount) == 0)
  }

  test("testGetMax") {
    val keeper = new Keeper(inputWarehouse, inputAmounts)
    val rightAmount = math.BigDecimal.valueOf(10.0)

    val amounts = keeper.getCurrentAmounts(keeper.getAmounts(ss))
    val warehouses = keeper.getWarehouses(ss)

    val maxs = keeper.getMax(warehouses, amounts)
      .filter(col("warehouse").===("W-1") && col("product").===("P-1")).collect().head
    val amount = maxs.get(2).asInstanceOf[math.BigDecimal]

    assert(amount.compareTo(rightAmount) == 0)

  }

  test("testParseFile returns actual DataFrame") {
    val keeper = new Keeper(inputWarehouse, inputAmounts)

    val itemsAmount = keeper.parseFile(ss, testPath, testSchema).count()
    assert(itemsAmount === 4)
  }

  test("testParseFile breaks on wrong file type") {
    val wrongWarehousePath = "/someWrongTypeInputPath.txt"
    val wrongAmountPath = "/anotherWrongInputPath.doc"
    val keeper = new Keeper(wrongWarehousePath, wrongAmountPath)
    intercept[FileTypeNotSupportedException] {
      keeper.parseFile(ss, wrongAmountPath, keeper.warehouseSchema)
    }
  }

  test("testGetAmounts") {
    val keeper = new Keeper(inputWarehouse, inputAmounts)

    val amounts = keeper.getAmounts(ss)
    val count = amounts.count()

    assert(count === 9)
  }

  test("testGetCurrentAmounts distinct ids") {
    val keeper = new Keeper(inputWarehouse, inputAmounts)

    val amounts = keeper.getAmounts(ss)
    val currentAmounts = keeper.getCurrentAmounts(amounts)

    assert(currentAmounts.count() < amounts.count())
  }

  test("testGetCurrentAmounts returns correct result") {
    val keeper = new Keeper(inputWarehouse, inputAmounts)

    val amounts = keeper.getAmounts(ss)
    val currentAmounts = keeper.getCurrentAmounts(amounts)

    assert(currentAmounts.count() === 5)
  }

  test("testGetWarehouses") {
    val keeper = new Keeper(inputWarehouse, inputAmounts)

    val warehouses = keeper.getWarehouses(ss)
    val count = warehouses.count()

    assert(count === 5)
  }

}

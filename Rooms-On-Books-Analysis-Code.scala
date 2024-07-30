// Databricks notebook source
// DBTITLE 1,Libraries
// Importing the required libraries
import org.apache.spark.sql.SparkSession
import com.crealytics.spark.excel._
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.apache.poi.ss.usermodel.WorkbookFactory
import java.io.{File, FileInputStream, FileOutputStream}
import scala.collection.JavaConverters._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.Row
import java.math.RoundingMode

// Initializing Spark session
val spark = SparkSession.builder.appName("RoomsOnBooks").getOrCreate()

// Path to the Excel file
val dbfsFilePath = "dbfs:/FileStore/Rooms_on_Books_08_07_24.xlsx"

// Defining the local file path
val localFilePath = "/tmp/Rooms_on_Books_08_07_24.xlsx"

// Copying the file from DBFS to local file system
dbutils.fs.cp(dbfsFilePath, "file:" + localFilePath)

// DBTITLE 1,Sheet names
// Function to get all sheet names from the Excel file
def getSheetNames(filePath: String): Seq[String] = {
  val inputStream = new FileInputStream(filePath)
  val workbook = WorkbookFactory.create(inputStream)
  val sheetNames = (0 until workbook.getNumberOfSheets).map(workbook.getSheetName)
  inputStream.close()
  sheetNames
}

// Getting all sheet names
val sheetNames = getSheetNames(localFilePath)

// DBTITLE 1,Initial Schema
// Defining the initial schema
val initialSchema = StructType(Seq(
  StructField("Date", StringType, true),
  StructField("RoB_23_Room_Revenues", DoubleType, true),
  StructField("RoB_24_Room_Revenues", DoubleType, true),
  StructField("Var1(%)_Room_Revenues", DoubleType, true), // Not necessary
  StructField("Actual_23_Room_Revenues", DoubleType, true),
  StructField("Budget_24_Room_Revenues", DoubleType, true),
  StructField("Var2(%)_Room_Revenues", DoubleType, true), // Not necessary
  StructField("Blank_Room_Revenues", DoubleType, true), // Not necessary
  StructField("RoB_23_Room_Nights", IntegerType, true),
  StructField("RoB_24_Room_Nights", IntegerType, true),
  StructField("Var1(%)_Room_Nights", DoubleType, true), // Not necessary
  StructField("Actual_23_Room_Nights", IntegerType, true),
  StructField("Budget_24_Room_Nights", IntegerType, true),
  StructField("Var2(%)_Room_Nights", DoubleType, true), // Not necessary
  StructField("Blank_Room_Nights", DoubleType, true), // Not necessary
  StructField("RoB_23_Available_Rooms", DoubleType, true), // Not necessary
  StructField("RoB_24_Available_Rooms", DoubleType, true), // Not necessary
  StructField("Var1(%)_Available_Rooms", DoubleType, true), // Not necessary
  StructField("Actual_23_Available_Rooms", DoubleType, true), // Not necessary
  StructField("Budget_24_Available_Rooms", DoubleType, true), // Not necessary
  StructField("Var2(%)_Available_Rooms", DoubleType, true), // Not necessary
  StructField("Blank_Available_Rooms", DoubleType, true), // Not necessary
  StructField("RoB_23_Occupancy", DoubleType, true), // Not necessary
  StructField("RoB_24_Occupancy", DoubleType, true), // Not necessary
  StructField("Var1(%)_Occupancy", DoubleType, true), // Not necessary
  StructField("Actual_23_Occupancy", DoubleType, true), // Not necessary
  StructField("Budget_24_Occupancy", DoubleType, true), // Not necessary
  StructField("Var2(%)_Occupancy", DoubleType, true), // Not necessary
  StructField("Blank_Occupancy", DoubleType, true), // Not necessary
  StructField("RoB_23_ADR", DoubleType, true),
  StructField("RoB_24_ADR", DoubleType, true),
  StructField("Var1(%)_ADR", DoubleType, true), // Not necessary
  StructField("Actual_23_ADR", DoubleType, true),
  StructField("Budget_24_ADR", DoubleType, true),
  StructField("Var2(%)_ADR", DoubleType, true) // Not necessary
))

// DBTITLE 1,Selecting Columns
// Defining the indices of the columns to be selected
val selectedIndices = Seq(1, 2, 3, 5, 6, 9, 10, 12, 13, 30, 31, 33, 34)

// Defining the new names for the selected columns
val newNames = Seq("Date", "RoB_23_Room_Revenues", "RoB_24_Room_Revenues", "Actual_23_Room_Revenues", "Budget_24_Room_Revenues", "RoB_23_Room_Nights", "RoB_24_Room_Nights", "Actual_23_Room_Nights", "Budget_24_Room_Nights", "RoB_23_ADR", "RoB_24_ADR", "Actual_23_ADR", "Budget_24_ADR")

// Function to transform data for each sheet
def transformData(df: DataFrame, nobleName: String): DataFrame = {
  val availableColumns = df.columns
  val validIndices = selectedIndices.filter(_ <= availableColumns.length)

  val selectedColumns = validIndices.map(i => s"`" + df.columns(i - 1) + "`")
  val selectedDF = df.select(selectedColumns.map(col): _*).toDF(newNames: _*).withColumn("GERUNDA", lit(nobleName))
  selectedDF
}

// DBTITLE 1,Dataframe 1
// Processing the first sheet and collecting the result
val sheet1Name = sheetNames(0)

val df1 = spark.read.format("com.crealytics.spark.excel")
  .schema(initialSchema)
  .option("sheetName", sheet1Name)
  .option("header", "true")
  .option("inferSchema", "false")
  .option("dataAddress", s"'$sheet1Name'!B4:AJ18")
  .load(dbfsFilePath)

val transformedDF1 = transformData(df1, sheet1Name)
val juneDF1 = transformedDF1.filter(col("Date") === "June")

juneDF1.show(1, 20, true)

// DBTITLE 1,Dataframe 2
// Processing the second sheet and collecting the result
val sheet2Name = sheetNames(1)

val df2 = spark.read.format("com.crealytics.spark.excel")
  .schema(initialSchema)
  .option("sheetName", sheet2Name)
  .option("header", "true")
  .option("inferSchema", "false")
  .option("dataAddress", s"'$sheet2Name'!B4:AJ18")
  .load(dbfsFilePath)

val transformedDF2 = transformData(df2, sheet2Name)
val juneDF2 = transformedDF2.filter(col("Date") === "June")

juneDF2.show(1, 20, true)

// DBTITLE 1,Dataframe 3
// Processing the third sheet and collecting the result
val sheet3Name = sheetNames(2)

val df3 = spark.read.format("com.crealytics.spark.excel")
  .schema(initialSchema)
  .option("sheetName", sheet3Name)
  .option("header", "true")
  .option("inferSchema", "false")
  .option("dataAddress", s"'$sheet3Name'!B4:AJ18")
  .load(dbfsFilePath)

val transformedDF3 = transformData(df3, sheet3Name)
val juneDF3 = transformedDF3.filter(col("Date") === "June")

juneDF3.show(1, 20, true)

// DBTITLE 1,Dataframe 4
// Processing the fourth sheet and collecting the result
val sheet4Name = sheetNames(3)

val df4 = spark.read.format("com.crealytics.spark.excel")
  .schema(initialSchema)
  .option("sheetName", sheet4Name)
  .option("header", "true")
  .option("inferSchema", "false")
  .option("dataAddress", s"'$sheet4Name'!B4:AJ18")
  .load(dbfsFilePath)

val transformedDF4 = transformData(df4, sheet4Name)
val juneDF4 = transformedDF4.filter(col("Date") === "June")

juneDF4.show(1, 20, true)

// DBTITLE 1,Dataframe 5
// Processing the fifth sheet and collecting the result
val sheet5Name = sheetNames(4)

val df5 = spark.read.format("com.crealytics.spark.excel")
  .schema(initialSchema)
  .option("sheetName", sheet5Name)
  .option("header", "true")
  .option("inferSchema", "false")
  .option("dataAddress", s"'$sheet5Name'!B4:AJ18")
  .load(dbfsFilePath)

val transformedDF5 = transformData(df5, sheet5Name)
val juneDF5 = transformedDF5.filter(col("Date") === "June")

juneDF5.show(1, 20, true)

// DBTITLE 1,Dataframe 6
// Processing the sixth sheet and collecting the result
val sheet6Name = sheetNames(5)

val df6 = spark.read.format("com.crealytics.spark.excel")
  .schema(initialSchema)
  .option("sheetName", sheet6Name)
  .option("header", "true")
  .option("inferSchema", "false")
  .option("dataAddress", s"'$sheet6Name'!B4:AJ18")
  .load(dbfsFilePath)

val transformedDF6 = transformData(df6, sheet6Name)
val juneDF6 = transformedDF6.filter(col("Date") === "June")

juneDF6.show(1, 20, true)

// DBTITLE 1,Dataframe 7
// Processing the seventh sheet and collecting the result
val sheet7Name = sheetNames(6)

val df7 = spark.read.format("com.crealytics.spark.excel")
  .schema(initialSchema)
  .option("sheetName", sheet7Name)
  .option("header", "true")
  .option("inferSchema", "false")
  .option("dataAddress", s"'$sheet7Name'!B4:AJ18")
  .load(dbfsFilePath)

val transformedDF7 = transformData(df7, sheet7Name)
val juneDF7 = transformedDF7.filter(col("Date") === "June")

juneDF7.show(1, 20, true)

// DBTITLE 1,Final Schema
// Defining the final schema
val schema = StructType(Seq(
  StructField("GERUNDA", StringType, true),
  StructField("RoB_23_Room_Revenues", DoubleType, true),
  StructField("RoB_24_Room_Revenues", DoubleType, true),
  StructField("Var1(%)_Room_Revenues", DoubleType, true),
  StructField("Actual_23_Room_Revenues", DoubleType, true),
  StructField("Budget_24_Room_Revenues", DoubleType, true),
  StructField("Var2(%)_Room_Revenues", DoubleType, true),
  StructField("To_go_to_budget_Room_Revenues", DoubleType, true),
  StructField("RoB_23_ADR", DoubleType, true),
  StructField("RoB_24_ADR", DoubleType, true),
  StructField("Var1(%)_ADR", DoubleType, true),
  StructField("Actual_23_ADR", DoubleType, true),
  StructField("Budget_24_ADR", DoubleType, true),
  StructField("Var2(%)_ADR", DoubleType, true),
  StructField("To_go_to_budget_ADR", DoubleType, true),
  StructField("RoB_24_Room_Nights", DoubleType, true),  
  StructField("RoB_23_Room_Nights", DoubleType, true),
  StructField("Actual_23_Room_Nights", DoubleType, true),
  StructField("Budget_24_Room_Nights", DoubleType, true)
))

// DBTITLE 1,Data for the new DF
// Function to safely extract Double values
def safeGetAsDouble(row: Row, columnName: String): Double = {
  if (row.isNullAt(row.fieldIndex(columnName))) null.asInstanceOf[Double]
  else row.getAs[Number](columnName).doubleValue()
}

val juneDF1Row = juneDF1.first()
val juneDF2Row = juneDF2.first()
val juneDF3Row = juneDF3.first()
val juneDF4Row = juneDF4.first()
val juneDF5Row = juneDF5.first()
val juneDF6Row = juneDF6.first()
val juneDF7Row = juneDF7.first()

// Creating the data for each DF
val data = Seq(
  Row(
    "Regente",
    safeGetAsDouble(juneDF1Row, "RoB_23_Room_Revenues"),
    safeGetAsDouble(juneDF1Row, "RoB_24_Room_Revenues"),
    null, // Placeholder for Var1(%)_Room_Revenues
    safeGetAsDouble(juneDF1Row, "Actual_23_Room_Revenues"),
    safeGetAsDouble(juneDF1Row, "Budget_24_Room_Revenues"),
    null, // Placeholder for Var2(%)_Room_Revenues
    null, // Placeholder for To_go_to_budget_Room_Revenues
    safeGetAsDouble(juneDF1Row, "RoB_23_ADR"),
    safeGetAsDouble(juneDF1Row, "RoB_24_ADR"),
    null, // Placeholder for Var1(%)_ADR
    safeGetAsDouble(juneDF1Row, "Actual_23_ADR"),
    safeGetAsDouble(juneDF1Row, "Budget_24_ADR"),
    null, // Placeholder for Var2(%)_ADR
    null, // Placeholder for To_go_to_budget_ADR
    safeGetAsDouble(juneDF1Row, "RoB_24_Room_Nights"),
    safeGetAsDouble(juneDF1Row, "RoB_23_Room_Nights"),
    safeGetAsDouble(juneDF1Row, "Actual_23_Room_Nights"),
    safeGetAsDouble(juneDF1Row, "Budget_24_Room_Nights")
  ),
  Row(
    "Rio Park",
    safeGetAsDouble(juneDF2Row, "RoB_23_Room_Revenues"),
    safeGetAsDouble(juneDF2Row, "RoB_24_Room_Revenues"),
    null, // Placeholder for Var1(%)_Room_Revenues
    safeGetAsDouble(juneDF2Row, "Actual_23_Room_Revenues"),
    safeGetAsDouble(juneDF2Row, "Budget_24_Room_Revenues"),
    null, // Placeholder for Var2(%)_Room_Revenues
    null, // Placeholder for To_go_to_budget_Room_Revenues
    safeGetAsDouble(juneDF2Row, "RoB_23_ADR"),
    safeGetAsDouble(juneDF2Row, "RoB_24_ADR"),
    null, // Placeholder for Var1(%)_ADR
    safeGetAsDouble(juneDF2Row, "Actual_23_ADR"),
    safeGetAsDouble(juneDF2Row, "Budget_24_ADR"),
    null, // Placeholder for Var2(%)_ADR
    null, // Placeholder for To_go_to_budget_ADR
    safeGetAsDouble(juneDF2Row, "RoB_24_Room_Nights"),
    safeGetAsDouble(juneDF2Row, "RoB_23_Room_Nights"),
    safeGetAsDouble(juneDF2Row, "Actual_23_Room_Nights"),
    safeGetAsDouble(juneDF2Row, "Budget_24_Room_Nights")
  ),
  Row(
    "Ruidor",
    safeGetAsDouble(juneDF3Row, "RoB_23_Room_Revenues"),
    safeGetAsDouble(juneDF3Row, "RoB_24_Room_Revenues"),
    null, // Placeholder for Var1(%)_Room_Revenues
    safeGetAsDouble(juneDF3Row, "Actual_23_Room_Revenues"),
    safeGetAsDouble(juneDF3Row, "Budget_24_Room_Revenues"),
    null, // Placeholder for Var2(%)_Room_Revenues
    null, // Placeholder for To_go_to_budget_Room_Revenues
    safeGetAsDouble(juneDF3Row, "RoB_23_ADR"),
    safeGetAsDouble(juneDF3Row, "RoB_24_ADR"),
    null, // Placeholder for Var1(%)_ADR
    safeGetAsDouble(juneDF3Row, "Actual_23_ADR"),
    safeGetAsDouble(juneDF3Row, "Budget_24_ADR"),
    null, // Placeholder for Var2(%)_ADR
    null, // Placeholder for To_go_to_budget_ADR
    safeGetAsDouble(juneDF3Row, "RoB_24_Room_Nights"),
    safeGetAsDouble(juneDF3Row, "RoB_23_Room_Nights"),
    safeGetAsDouble(juneDF3Row, "Actual_23_Room_Nights"),
    safeGetAsDouble(juneDF3Row, "Budget_24_Room_Nights")
  ),
  Row(
    "Flamingo",
    safeGetAsDouble(juneDF4Row, "RoB_23_Room_Revenues"),
    safeGetAsDouble(juneDF4Row, "RoB_24_Room_Revenues"),
    null, // Placeholder for Var1(%)_Room_Revenues
    safeGetAsDouble(juneDF4Row, "Actual_23_Room_Revenues"),
    safeGetAsDouble(juneDF4Row, "Budget_24_Room_Revenues"),
    null, // Placeholder for Var2(%)_Room_Revenues
    null, // Placeholder for To_go_to_budget_Room_Revenues
    safeGetAsDouble(juneDF4Row, "RoB_23_ADR"),
    safeGetAsDouble(juneDF4Row, "RoB_24_ADR"),
    null, // Placeholder for Var1(%)_ADR
    safeGetAsDouble(juneDF4Row, "Actual_23_ADR"),
    safeGetAsDouble(juneDF4Row, "Budget_24_ADR"),
    null, // Placeholder for Var2(%)_ADR
    null, // Placeholder for To_go_to_budget_ADR
    safeGetAsDouble(juneDF4Row, "RoB_24_Room_Nights"),
    safeGetAsDouble(juneDF4Row, "RoB_23_Room_Nights"),
    safeGetAsDouble(juneDF4Row, "Actual_23_Room_Nights"),
    safeGetAsDouble(juneDF4Row, "Budget_24_Room_Nights")
  ),
   Row(
    "Agir",
    safeGetAsDouble(juneDF5Row, "RoB_23_Room_Revenues"),
    safeGetAsDouble(juneDF5Row, "RoB_24_Room_Revenues"),
    null, // Placeholder for Var1(%)_Room_Revenues
    safeGetAsDouble(juneDF5Row, "Actual_23_Room_Revenues"),
    safeGetAsDouble(juneDF5Row, "Budget_24_Room_Revenues"),
    null, // Placeholder for Var2(%)_Room_Revenues
    null, // Placeholder for To_go_to_budget_Room_Revenues
    safeGetAsDouble(juneDF5Row, "RoB_23_ADR"),
    safeGetAsDouble(juneDF5Row, "RoB_24_ADR"),
    null, // Placeholder for Var1(%)_ADR
    safeGetAsDouble(juneDF5Row, "Actual_23_ADR"),
    safeGetAsDouble(juneDF5Row, "Budget_24_ADR"),
    null, // Placeholder for Var2(%)_ADR
    null, // Placeholder for To_go_to_budget_ADR
    safeGetAsDouble(juneDF5Row, "RoB_24_Room_Nights"),
    safeGetAsDouble(juneDF5Row, "RoB_23_Room_Nights"),
    safeGetAsDouble(juneDF5Row, "Actual_23_Room_Nights"),
    safeGetAsDouble(juneDF5Row, "Budget_24_Room_Nights")
  ),
  Row(
    "Pez Espada",
    safeGetAsDouble(juneDF6Row, "RoB_23_Room_Revenues"),
    safeGetAsDouble(juneDF6Row, "RoB_24_Room_Revenues"),
    null, // Placeholder for Var1(%)_Room_Revenues
    safeGetAsDouble(juneDF6Row, "Actual_23_Room_Revenues"),
    safeGetAsDouble(juneDF6Row, "Budget_24_Room_Revenues"),
    null, // Placeholder for Var2(%)_Room_Revenues
    null, // Placeholder for To_go_to_budget_Room_Revenues
    safeGetAsDouble(juneDF6Row, "RoB_23_ADR"),
    safeGetAsDouble(juneDF6Row, "RoB_24_ADR"),
    null, // Placeholder for Var1(%)_ADR
    safeGetAsDouble(juneDF6Row, "Actual_23_ADR"),
    safeGetAsDouble(juneDF6Row, "Budget_24_ADR"),
    null, // Placeholder for Var2(%)_ADR
    null, // Placeholder for To_go_to_budget_ADR
    safeGetAsDouble(juneDF6Row, "RoB_24_Room_Nights"),
    safeGetAsDouble(juneDF6Row, "RoB_23_Room_Nights"),
    safeGetAsDouble(juneDF6Row, "Actual_23_Room_Nights"),
    safeGetAsDouble(juneDF6Row, "Budget_24_Room_Nights")
  ),
  Row(
    "Riviera",
    safeGetAsDouble(juneDF7Row, "RoB_23_Room_Revenues"),
    safeGetAsDouble(juneDF7Row, "RoB_24_Room_Revenues"),
    null, // Placeholder for Var1(%)_Room_Revenues
    safeGetAsDouble(juneDF7Row, "Actual_23_Room_Revenues"),
    safeGetAsDouble(juneDF7Row, "Budget_24_Room_Revenues"),
    null, // Placeholder for Var2(%)_Room_Revenues
    null, // Placeholder for To_go_to_budget_Room_Revenues
    safeGetAsDouble(juneDF7Row, "RoB_23_ADR"),
    safeGetAsDouble(juneDF7Row, "RoB_24_ADR"),
    null, // Placeholder for Var1(%)_ADR
    safeGetAsDouble(juneDF7Row, "Actual_23_ADR"),
    safeGetAsDouble(juneDF7Row, "Budget_24_ADR"),
    null, // Placeholder for Var2(%)_ADR
    null, // Placeholder for To_go_to_budget_ADR
    safeGetAsDouble(juneDF7Row, "RoB_24_Room_Nights"),
    safeGetAsDouble(juneDF7Row, "RoB_23_Room_Nights"),
    safeGetAsDouble(juneDF7Row, "Actual_23_Room_Nights"),
    safeGetAsDouble(juneDF7Row, "Budget_24_Room_Nights")
  ),
  Row(
    "TOTAL",
    null, // Placeholder for total_RoB_23_Room_Revenues
    null, // Placeholder for total_RoB_24_Room_Revenues
    null, // Placeholder for Var1(%)_Room_Revenues
    null, // Placeholder for total_Actual_23_Room_Revenues
    null, // Placeholder for total_Budget_24_Room_Revenues
    null, // Placeholder for Var2(%)_Room_Revenues
    null, // Placeholder for To_go_to_budget_Room_Revenues
    null, // Placeholder for total_RoB_23_ADR
    null, // Placeholder for total_RoB_24_ADR
    null, // Placeholder for Var1(%)_ADR
    null, // Placeholder for total_Actual_23_ADR
    null, // Placeholder for total_Budget_24_ADR
    null, // Placeholder for Var2(%)_ADR
    null, // Placeholder for To_go_to_budget_ADR
    null, // Placeholder for total_RoB_24_Room_Nights
    null, // Placeholder for total_RoB_23_Room_Nights
    null, // Placeholder for total_Actual_23_Room_Nights
    null // Placeholder for total_Budget_24_Room_Nights
  )
)

// Creating the new DF with the specified data
val newDF = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
newDF.show(4, 20, true)

// DBTITLE 1,Adding missing values to the DF
// Function to calculate "Var1(%)_Room_Revenues" and format as percentage integer
def calculateVar1RoomRevenues(df: DataFrame): DataFrame = {
  df.withColumn("Var1(%)_Room_Revenues", expr("CASE WHEN RoB_23_Room_Revenues IS NULL OR RoB_24_Room_Revenues IS NULL THEN NULL ELSE CONCAT(CAST(bround((RoB_24_Room_Revenues / RoB_23_Room_Revenues - 1) * 100, 0) AS INT), '%') END"))
}

// Function to calculate "Var2(%)_Room_Revenues" and format as percentage integer
def calculateVar2RoomRevenues(df: DataFrame): DataFrame = {
  df.withColumn("Var2(%)_Room_Revenues", expr("CASE WHEN Actual_23_Room_Revenues IS NULL OR Budget_24_Room_Revenues IS NULL THEN NULL ELSE CONCAT(CAST(bround((Budget_24_Room_Revenues / Actual_23_Room_Revenues - 1) * 100, 0) AS INT), '%') END"))
}

// Function to calculate "To_go_to_budget_Room_Revenues" and format as percentage integer 
def calculateToGoToBudgetRoomRevenues(df: DataFrame): DataFrame = {
  df.withColumn("To_go_to_budget_Room_Revenues", expr("CASE WHEN Budget_24_Room_Revenues IS NULL OR RoB_24_Room_Revenues IS NULL THEN NULL ELSE CONCAT(CAST(bround((RoB_24_Room_Revenues / Budget_24_Room_Revenues - 1) * 100, 0) AS INT), '%') END"))
}

// Function to calculate "Var1(%)_ADR" and format as percentage integer
def calculateVar1ADR(df: DataFrame): DataFrame = {
  df.withColumn("Var1(%)_ADR", expr("CASE WHEN RoB_23_ADR IS NULL OR RoB_24_ADR IS NULL THEN NULL ELSE CONCAT(CAST(bround((RoB_24_ADR / RoB_23_ADR - 1) * 100, 0) AS INT), '%') END"))
}

// Function to calculate "Var2(%)_ADR" and format as percentage integer
def calculateVar2ADR(df: DataFrame): DataFrame = {
  df.withColumn("Var2(%)_ADR", expr("CASE WHEN Actual_23_ADR IS NULL OR Budget_24_ADR IS NULL THEN NULL ELSE CONCAT(CAST(bround((Budget_24_ADR / Actual_23_ADR - 1) * 100, 0) AS INT), '%') END"))
}

// Function to calculate "To_go_to_budget_ADR" and format as percentage integer
def calculateToGoToBudgetADR(df: DataFrame): DataFrame = {
  df.withColumn("To_go_to_budget_ADR", expr("CASE WHEN Budget_24_ADR IS NULL OR RoB_24_ADR IS NULL THEN NULL ELSE CONCAT(CAST(bround((RoB_24_ADR / Budget_24_ADR - 1) * 100, 0) AS INT), '%') END"))
}

// Applying the functions to the new DataFrame
val newDFWithVars = newDF
  .transform(calculateVar1RoomRevenues)
  .transform(calculateVar2RoomRevenues)
  .transform(calculateToGoToBudgetRoomRevenues)
  .transform(calculateVar1ADR)
  .transform(calculateVar2ADR)
  .transform(calculateToGoToBudgetADR)

newDFWithVars.show(4, 20, true)

// DBTITLE 1,Total values
// Calculating the totals of the specified columns
val totals = newDFWithVars.agg(
  sum("RoB_23_Room_Revenues").as("total_RoB_23_Room_Revenues"),
  sum("RoB_24_Room_Revenues").as("total_RoB_24_Room_Revenues"),
  sum("Actual_23_Room_Revenues").as("total_Actual_23_Room_Revenues"),
  sum("Budget_24_Room_Revenues").as("total_Budget_24_Room_Revenues"),  
  sum("RoB_24_Room_Nights").as("total_RoB_24_Room_Nights"),
  sum("RoB_23_Room_Nights").as("total_RoB_23_Room_Nights"),
  sum("Actual_23_Room_Nights").as("total_Actual_23_Room_Nights"),
  sum("Budget_24_Room_Nights").as("total_Budget_24_Room_Nights")
).first()

// Extracting the totals
val total_RoB_23_Room_Revenues = Option(totals.getAs[Double]("total_RoB_23_Room_Revenues")).getOrElse(0.0)
val total_RoB_24_Room_Revenues = Option(totals.getAs[Double]("total_RoB_24_Room_Revenues")).getOrElse(0.0)
val total_Actual_23_Room_Revenues = Option(totals.getAs[Double]("total_Actual_23_Room_Revenues")).getOrElse(0.0)
val total_Budget_24_Room_Revenues = Option(totals.getAs[Double]("total_Budget_24_Room_Revenues")).getOrElse(0.0)
val total_RoB_24_Room_Nights = Option(totals.getAs[Double]("total_RoB_24_Room_Nights")).getOrElse(0.0)
val total_RoB_23_Room_Nights = Option(totals.getAs[Double]("total_RoB_23_Room_Nights")).getOrElse(0.0)
val total_Actual_23_Room_Nights = Option(totals.getAs[Double]("total_Actual_23_Room_Nights")).getOrElse(0.0)
val total_Budget_24_Room_Nights = Option(totals.getAs[Double]("total_Budget_24_Room_Nights")).getOrElse(0.0)

// Calculating ADR values
val total_RoB_23_ADR = if (total_RoB_23_Room_Nights != 0) total_RoB_23_Room_Revenues / total_RoB_23_Room_Nights else 0.0
val total_RoB_24_ADR = if (total_RoB_24_Room_Nights != 0) total_RoB_24_Room_Revenues / total_RoB_24_Room_Nights else 0.0
val total_var1_ADR = if (total_RoB_23_ADR != 0) ((total_RoB_24_ADR / total_RoB_23_ADR - 1) * 100).round else 0.0
val total_Actual_23_ADR = if (total_Actual_23_Room_Nights != 0) total_Actual_23_Room_Revenues / total_Actual_23_Room_Nights else 0.0
val total_Budget_24_ADR = if (total_Budget_24_Room_Nights != 0) total_Budget_24_Room_Revenues / total_Budget_24_Room_Nights else 0.0
val total_var2_ADR = if (total_Actual_23_ADR != 0) ((total_Budget_24_ADR / total_Actual_23_ADR - 1) * 100).round else 0.0
val total_to_go_to_budget_ADR = if (total_Budget_24_ADR != 0) ((total_RoB_24_ADR / total_Budget_24_ADR - 1) * 100).round else 0.0

// Calculating the percentage values
val total_var1_Room_Revenues = if (total_RoB_23_Room_Revenues != 0) ((total_RoB_24_Room_Revenues / total_RoB_23_Room_Revenues - 1) * 100).round else 0.0
val total_var2_Room_Revenues = if (total_Actual_23_Room_Revenues != 0) ((total_Budget_24_Room_Revenues / total_Actual_23_Room_Revenues - 1) * 100).round else 0.0
val total_to_go_to_budget_Room_Revenues = if (total_Budget_24_Room_Revenues != 0) ((total_RoB_24_Room_Revenues / total_Budget_24_Room_Revenues - 1) * 100).round else 0.0

// DBTITLE 1,Updating DF with total row
// Updating the TOTAL row in the DF
val updatedDF = newDFWithVars
  .withColumn("RoB_23_Room_Revenues", expr(s"CASE WHEN `GERUNDA` = 'TOTAL' THEN $total_RoB_23_Room_Revenues ELSE `RoB_23_Room_Revenues` END"))
  .withColumn("RoB_24_Room_Revenues", expr(s"CASE WHEN `GERUNDA` = 'TOTAL' THEN $total_RoB_24_Room_Revenues ELSE `RoB_24_Room_Revenues` END"))
  .withColumn("Var1(%)_Room_Revenues", expr(s"CASE WHEN `GERUNDA` = 'TOTAL' THEN CONCAT($total_var1_Room_Revenues, '%') ELSE `Var1(%)_Room_Revenues` END"))
  .withColumn("Actual_23_Room_Revenues", expr(s"CASE WHEN `GERUNDA` = 'TOTAL' THEN $total_Actual_23_Room_Revenues ELSE `Actual_23_Room_Revenues` END"))
  .withColumn("Budget_24_Room_Revenues", expr(s"CASE WHEN `GERUNDA` = 'TOTAL' THEN $total_Budget_24_Room_Revenues ELSE `Budget_24_Room_Revenues` END"))
  .withColumn("Var2(%)_Room_Revenues", expr(s"CASE WHEN `GERUNDA` = 'TOTAL' THEN CONCAT($total_var2_Room_Revenues, '%') ELSE `Var2(%)_Room_Revenues` END"))
  .withColumn("To_go_to_budget_Room_Revenues", expr(s"CASE WHEN `GERUNDA` = 'TOTAL' THEN CONCAT($total_to_go_to_budget_Room_Revenues, '%') ELSE `To_go_to_budget_Room_Revenues` END"))
  .withColumn("RoB_23_ADR", expr(s"CASE WHEN `GERUNDA` = 'TOTAL' THEN $total_RoB_23_ADR ELSE `RoB_23_ADR` END"))
  .withColumn("RoB_24_ADR", expr(s"CASE WHEN `GERUNDA` = 'TOTAL' THEN $total_RoB_24_ADR ELSE `RoB_24_ADR` END"))
  .withColumn("Var1(%)_ADR", expr(s"CASE WHEN `GERUNDA` = 'TOTAL' THEN CONCAT($total_var1_ADR, '%') ELSE `Var1(%)_ADR` END"))
  .withColumn("Actual_23_ADR", expr(s"CASE WHEN `GERUNDA` = 'TOTAL' THEN $total_Actual_23_ADR ELSE `Actual_23_ADR` END"))
  .withColumn("Budget_24_ADR", expr(s"CASE WHEN `GERUNDA` = 'TOTAL' THEN $total_Budget_24_ADR ELSE `Budget_24_ADR` END"))
  .withColumn("Var2(%)_ADR", expr(s"CASE WHEN `GERUNDA` = 'TOTAL' THEN CONCAT($total_var2_ADR, '%') ELSE `Var2(%)_ADR` END"))
  .withColumn("To_go_to_budget_ADR", expr(s"CASE WHEN `GERUNDA` = 'TOTAL' THEN CONCAT($total_to_go_to_budget_ADR, '%') ELSE `To_go_to_budget_ADR` END"))
  .withColumn("RoB_24_Room_Nights", expr(s"CASE WHEN `GERUNDA` = 'TOTAL' THEN $total_RoB_24_Room_Nights ELSE `RoB_24_Room_Nights` END"))
  .withColumn("RoB_23_Room_Nights", expr(s"CASE WHEN `GERUNDA` = 'TOTAL' THEN $total_RoB_23_Room_Nights ELSE `RoB_23_Room_Nights` END"))
  .withColumn("Actual_23_Room_Nights", expr(s"CASE WHEN `GERUNDA` = 'TOTAL' THEN $total_Actual_23_Room_Nights ELSE `Actual_23_Room_Nights` END"))
  .withColumn("Budget_24_Room_Nights", expr(s"CASE WHEN `GERUNDA` = 'TOTAL' THEN $total_Budget_24_Room_Nights ELSE `Budget_24_Room_Nights` END"))

updatedDF.show(3, 20, true)

// DBTITLE 1,Percentage formatting
// Function to format percentage values with parentheses for negative values
def formatPercentageWithParentheses(df: DataFrame, columnName: String): DataFrame = {
  df.withColumn(columnName, expr(s"""
    CASE 
      WHEN `$columnName` IS NULL THEN NULL 
      ELSE 
        CASE 
          WHEN CAST(SUBSTRING(`$columnName`, 1, LENGTH(`$columnName`) - 1) AS DOUBLE) < 0 
          THEN CONCAT('(', ABS(FLOOR(CAST(SUBSTRING(`$columnName`, 1, LENGTH(`$columnName`) - 1) AS DOUBLE))), '%', ')') 
          ELSE CONCAT(FLOOR(CAST(SUBSTRING(`$columnName`, 1, LENGTH(`$columnName`) - 1) AS DOUBLE)), '%') 
        END 
    END"""))
}

val finalDF = updatedDF
  .transform(df => formatPercentageWithParentheses(df, "Var1(%)_Room_Revenues"))
  .transform(df => formatPercentageWithParentheses(df, "Var2(%)_Room_Revenues"))
  .transform(df => formatPercentageWithParentheses(df, "To_go_to_budget_Room_Revenues"))
  .transform(df => formatPercentageWithParentheses(df, "Var1(%)_ADR"))
  .transform(df => formatPercentageWithParentheses(df, "Var2(%)_ADR"))
  .transform(df => formatPercentageWithParentheses(df, "To_go_to_budget_ADR"))

finalDF.show(3, 20, true)

// DBTITLE 1,Removing decimal cases
// Function to remove decimal part from numeric columns and convert to string
def formatNumericColumns(df: DataFrame): DataFrame = {
  df.schema.fields.foldLeft(df) { (accDF, field) =>
    field.dataType match {
      case _: DecimalType => accDF.withColumn(field.name, round(col(field.name), 0).cast(LongType).cast(StringType))
      case _: DoubleType => accDF.withColumn(field.name, round(col(field.name), 0).cast(LongType).cast(StringType))
      case _: FloatType => accDF.withColumn(field.name, round(col(field.name), 0).cast(LongType).cast(StringType))
      case _: IntegerType => accDF.withColumn(field.name, col(field.name).cast(LongType).cast(StringType))
      case _: LongType => accDF.withColumn(field.name, col(field.name).cast(LongType).cast(StringType))
      case _ => accDF
    }
  }
}

val formattedDF = formatNumericColumns(finalDF)
formattedDF.show(12, 20, true)

// DBTITLE 1,Ordering rows
// Defining the correct order for the GERUNDA column
val orderedGerundaNames = Seq("Flamingo", "Rio Park", "Regente", "Ruidor", "Agir", "Pez Espada", "Riviera", "TOTAL")

// Creating a DF with the order
val orderDF = orderedGerundaNames.zipWithIndex.map {
  case (name, index) => (name, index)
}.toDF("GERUNDA", "GERUNDA_order")

// Joining with the original DataFrame
val orderedDF = formattedDF.join(orderDF, Seq("GERUNDA"), "left").orderBy("GERUNDA_order").drop("GERUNDA_order")
orderedDF.show(12, 20, true)

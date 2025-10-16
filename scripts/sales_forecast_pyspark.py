from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, dayofmonth, month, year, to_date, to_timestamp, weekofyear, dayofweek
)
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

# -----------------------------
# 1) Initialize Spark session
# -----------------------------
my_spark = SparkSession.builder.appName("SalesForecast").getOrCreate()

# -----------------------------
# 2) Load sales data (CSV)
# -----------------------------
# NOTE: inferSchema=True tries to infer numeric types automatically.
sales_data = my_spark.read.csv(
    ["data/Online_Retail_part1.csv", "data/Online_Retail_part2.csv"],
    header=True,
    inferSchema=True,
    sep=","
)

# -----------------------------------------
# 3) Parse InvoiceDate into proper date type
# -----------------------------------------
sales_data = sales_data.withColumn(
    "InvoiceDate",
    to_date(to_timestamp(col("InvoiceDate"), "d/M/yyyy H:mm"))
)

# ---------------------------------------------------------
# 4) Create calendar features used by the ML model/aggregation
# ---------------------------------------------------------
sales_data = (
    sales_data
    .withColumn("Year", year(col("InvoiceDate")))
    .withColumn("Month", month(col("InvoiceDate")))
    .withColumn("Day", dayofmonth(col("InvoiceDate")))
    .withColumn("Week", weekofyear(col("InvoiceDate")))
    .withColumn("DayOfWeek", dayofweek(col("InvoiceDate")))
)

# ---------------------------------------------------------------------
# 5) Aggregate to daily level per Country x StockCode (sum qty, avg price)
# ---------------------------------------------------------------------
sales_data_agg = (
    sales_data
    .groupBy("Country", "StockCode", "InvoiceDate", "Year", "Month", "Day", "Week", "DayOfWeek")
    .agg({'Quantity': 'sum', 'UnitPrice': 'mean'})
    .withColumnRenamed('sum(Quantity)', 'Quantity')
    .withColumnRenamed('avg(UnitPrice)', 'UnitPrice')
)

# (Optional quick peek)
# sales_data_agg.limit(10).toPandas()

# --------------------------------------------------------------
# 6) Train/Test split at 2011-09-25 (<= train, > test as required)
# --------------------------------------------------------------
train_data = sales_data_agg.filter(sales_data_agg.InvoiceDate <= "2011-09-25")
test_data  = sales_data_agg.filter(sales_data_agg.InvoiceDate  > "2011-09-25")

# -----------------------------------------------------------
# 7) Convert train_data (Spark DF) to pandas DF
#    Contains at least: Country, StockCode, InvoiceDate, Quantity
# -----------------------------------------------------------
pd_daily_train_data = (
    train_data
    .select("Country", "StockCode", "InvoiceDate", "Quantity")
    .toPandas()
)

# (Optional quick peeks)
# train_data.limit(10).toPandas()
# test_data.limit(10).toPandas()

# ----------------------------------------------------
# 8) Feature engineering and model pipeline definition
# ----------------------------------------------------
# Index categorical variables (handle unseen labels with "keep")
country_indexer = (
    StringIndexer(inputCol="Country", outputCol="CountryIndex")
    .setHandleInvalid("keep")
)
stock_code_indexer = (
    StringIndexer(inputCol="StockCode", outputCol="StockCodeIndex")
    .setHandleInvalid("keep")
)

# Assemble numeric + indexed categorical features (no target leakage)
assembler = VectorAssembler(
    inputCols=["CountryIndex", "StockCodeIndex", "UnitPrice", "Year", "Month", "Day", "DayOfWeek"],
    outputCol="features"
)

# Random Forest Regressor to predict 'Quantity'
rf = RandomForestRegressor(
    featuresCol="features",
    labelCol="Quantity",
    maxBins=4000  # suitable for high-cardinality categorical bins
)

# Build the Pipeline
pipeline = Pipeline(stages=[country_indexer, stock_code_indexer, assembler, rf])

# ---------------------------------------
# 9) Fit the model on the training dataset
# ---------------------------------------
model = pipeline.fit(train_data)

# -----------------------------------------
# 10) Generate predictions on the test set
# -----------------------------------------
test_predictions = model.transform(test_data)
test_predictions = test_predictions.withColumn("prediction", col("prediction").cast("double"))

# -----------------------------------------
# 11) Evaluate using Mean Absolute Error (MAE)
# -----------------------------------------
mae_evaluator = RegressionEvaluator(
    labelCol="Quantity",
    predictionCol="prediction",
    metricName="mae"
)
mae = mae_evaluator.evaluate(test_predictions)
print(f"Mean Absolute Error (MAE): {mae}")

# --------------------------------------------------------------------
# 12) Global forecast for Week 39 of 2011 (sum of predicted quantities)
# --------------------------------------------------------------------
weekly_sales = test_predictions.groupBy("Year", "Week").agg({"prediction": "sum"})
week_39_sales = weekly_sales.filter(
    (weekly_sales.Year == 2011) & (weekly_sales.Week == 39)
)

# Extract scalar value -> Python int
quantity_sold_w39 = int(week_39_sales.select("sum(prediction)").collect()[0][0])
print(f"Predicted total quantity sold globally during week 39 of 2011: {quantity_sold_w39}")

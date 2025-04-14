# Import necessary libraries for Spark and AWS Glue functionalities
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession 
from pyspark.sql.functions import (
    to_timestamp, to_date, date_add, date_sub, datediff, months_between, add_months,
    year, month, dayofmonth, dayofweek, dayofyear, weekofyear, quarter,
    hour, minute, second, current_date, current_timestamp, now,expr)
    

# Initialize Spark context with log level
sc = SparkContext()
sc.setLogLevel("INFO")  # Setting log level for Spark context

glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Read the CSV file from S3

s3_path = "s3://ti-student-feb-2025/retail/dispatch/dispatch_transactions.csv"
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(s3_path)

# Convert 'dispatch_date' to TimestampType
df_prepared = df.withColumn("parsed_date", to_timestamp(df["dispatch_date"], "dd-MMM-yyyy HH:mm:ss"))
print("Dataframe with parsed date:")
df_prepared.show()

# Add current date and timestamp
df_prepared = df_prepared.withColumn("current_date", current_date())
df_prepared = df_prepared.withColumn("current_timestamp", current_timestamp())
df_prepared = df_prepared.withColumn("now_value", now())
print("Dataframe with current date:")
df_prepared.show()

# Add and subtract days
df_add_days = df_prepared.withColumn("date_plus_10_days", date_add("parsed_date", 10))
df_sub_days = df_add_days.withColumn("date_minus_10_days", date_sub("parsed_date", 10))
print("Dataframe with date added and date subtracted:")
df_sub_days.show()

# Extract parts of the date
df_extracted = df_prepared.withColumn("year", year("parsed_date"))
df_extracted = df_prepared.withColumn("month", month("parsed_date"))
df_extracted = df_prepared.withColumn("day", dayofmonth("parsed_date"))
df_extracted = df_prepared.withColumn("day_of_week", dayofweek("parsed_date"))
df_extracted = df_extracted.withColumn("day_of_year", dayofyear("parsed_date"))
df_extracted = df_extracted.withColumn("week_of_year", weekofyear("parsed_date"))
df_extracted = df_extracted.withColumn("quarter", quarter("parsed_date"))
df_extracted = df_extracted.withColumn("hour", hour("parsed_date"))
df_extracted = df_extracted.withColumn("minute", minute("parsed_date"))
df_extracted = df_extracted.withColumn("second", second("parsed_date"))
print("Dataframe showing parts of date:")
df_extracted.show()



# Calculate the difference in years from the dispatch date to now
df_date_diff = df_extracted.withColumn("years_from_now", expr("floor(datediff(current_date(), parsed_date)/365)"))
print("Dataframe showing date difference:")
df_date_diff.show()

# Log and print messages for each significant operation
print("Initial data loaded and date parsed.")
print("Fetched current date and timestamp")
print("Added and subtracted 10 days from the original date.")
print("Extracted parts from the date.")
print("Calculated the number of years from the current date to each dispatch date.")


# Log information after displaying the results
glueContext.get_logger().info("CTE query executed and results displayed successfully.")

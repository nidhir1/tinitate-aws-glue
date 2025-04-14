from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession

# Initialize Spark context with log level
sc = SparkContext()
sc.setLogLevel("INFO")  # Setting log level for Spark context to INFO

glueContext = GlueContext(sc)
spark = glueContext.spark_session
# Read the CSV file from S3
s3_path = "s3://ti-student-feb-2025/ti-athena-training/electric-vehicles/electric_vechiles/Electric_Vehicle_Population_Data.csv"
df= spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(s3_path)
df.printSchema()

# Define the partitioning columns
partition_columns = ["model year", "make"]

# Base path for S3 bucket where data will be stored
s3_base_path = "s3://ti-student-feb-2025/glue/scripts/student-ti-pyspark-s3csv-partitions.py"

# Write the DataFrame to S3 in Parquet format, partitioned by specified columns
df.write.partitionBy(*partition_columns).format("parquet").mode("overwrite").save(s3_base_path + "parquet/")

# Write the DataFrame to S3 in JSON format, partitioned by specified columns
df.write.partitionBy(*partition_columns).format("json").mode("overwrite").save(s3_base_path + "json/")

# Write the DataFrame to S3 in CSV format, partitioned by specified columns
df.write.partitionBy(*partition_columns).format("csv").mode("overwrite").option("header", "true").save(s3_base_path + "csv/")

# Print the schema of the DataFrame to verify the structure
df.printSchema()

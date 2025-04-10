from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession

# Initialize Spark context and Glue context
sc = SparkContext()
sc.setLogLevel("INFO")
glueContext = GlueContext(sc)

# Create or get an existing Spark session
spark = glueContext.spark_session


# Read the CSV file from S3

s3_path = "s3://ti-student-feb-2025/retail/product/products.csv"
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(s3_path)

# Calculate the distinct count of the bucketing column
bucket_column = "categoryid"
distinct_count = df.select(bucket_column).distinct().count()

# Define the base path in S3 where bucketed data will be stored

base_path = "s3://ti-student-feb-2025/glue/scripts/outputs/ti-pyspark-s3csv-bucketing-output/"

# Set the number of buckets based on the distinct count
num_buckets = min(distinct_count, 5)  # Adjust maximum number as appropriate

# Writing DataFrame bucketed by 'category' column
df.write.format("parquet") \
    .bucketBy(num_buckets, bucket_column) \
    .mode("overwrite") \
    .saveAsTable("bucketed_products", path=base_path)

# Read the bucketed data back to verify
bucketed_df = spark.read.parquet(base_path)

# Show schema to verify bucketing and see some data
bucketed_df.printSchema()
bucketed_df.show()

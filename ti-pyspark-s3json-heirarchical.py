from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import col, explode, lit
from pyspark.sql import SparkSession

# Initialize Glue/Spark context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Your nested JSON file in S3
s3_path = "s3://ti-student-feb-2025/nidhi/input/nested_json.json"  

# Read the hierarchical JSON
df = spark.read.option("multiline", "true").json(s3_path)

# Show original schema
print("Original Schema:")
df.printSchema()

# Explode coffee.region
coffee_region = df.select(
    explode("coffee.region").alias("region"),
    col("coffee.country.id").alias("coffee_country_id"),
    col("coffee.country.company").alias("coffee_country_company")
).withColumn("source", lit("coffee"))
print("Schema for coffee_region")
coffee_region.printSchema()
coffee_region.show(truncate=False)

# Explode brewing.region
brewing_region = df.select(
    explode("brewing.region").alias("region"),
    col("brewing.country.id").alias("brewing_country_id"),
    col("brewing.country.company").alias("brewing_country_company")
).withColumn("source", lit("brewing"))
print("Schema for Brewing_region")
brewing_region.printSchema()
brewing_region.show(truncate=False)


# Combine both exploded results (optional)
combined_df = coffee_region.select(
    col("region.id").alias("region_id"),
    col("region.name").alias("region_name"),
    col("coffee_country_id"),
    col("coffee_country_company"),
    col("source")
).unionByName(
    brewing_region.select(
        col("region.id").alias("region_id"),
        col("region.name").alias("region_name"),
        col("brewing_country_id").alias("coffee_country_id"),  # unify column names
        col("brewing_country_company").alias("coffee_country_company"),
        col("source")
    )
)

# Show flattened result
print("Flattened Result:")
combined_df.show(truncate=False)

# Log completion
glueContext.get_logger().info("Nested JSON from 'coffee' and 'brewing' flattened successfully.")

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Initialize Spark and Glue Contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Define the schema for our DataFrame
schema = StructType([
    StructField("category_id", IntegerType(), True),
    StructField("categoryname", StringType(), True),
    StructField("description", StringType(), True)
])

# Create sample data including null values
data = [
    (1, "Electronics", "Devices and gadgets"),
    (2, None, None),
    (3, "Clothing", "Wearables"),
    (4, "Home and Garden", "Home essentials"),
    (5, None, "All items for the kitchen")
]

# Create DataFrame using the data and schema defined
df = spark.createDataFrame(data, schema=schema)

# Show the original DataFrame
print("Displaying original DataFrame:")
df.show()

# Filter for null values in the 'categoryname' column
df_null = df.filter(col("categoryname").isNull())

# Display DataFrame with null values in 'categoryname'
print("Displaying DataFrame with null values in 'categoryname':")
df_null.show()

# Filter for not null values in the 'categoryname' column
df_not_null = df.filter(col("categoryname").isNotNull())

# Display DataFrame with non-null values in 'categoryname'
print("Displaying DataFrame with non-null values in 'categoryname':")
df_not_null.show()

# Log information using GlueContext
glueContext.get_logger().info("Completed displaying null and non-null values.")

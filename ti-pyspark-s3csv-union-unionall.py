from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession

# Initialize Spark context with log level
sc = SparkContext()
sc.setLogLevel("INFO")  # Setting log level for Spark context

glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Load CSV data into a DataFrame (For local testing, ensure the CSV path is correct and accessible)
df1 = spark.read.option("header", "true").csv("s3://ti-student-feb-2025/retail/product/products.csv")  # Local CSV path

df2 = spark.read.option("header", "true").csv("s3://ti-student-feb-2025/retail/product/products_un_in.csv")  # Local CSV path

# Union operation (remove duplicates)
union_df = df1.union(df2).distinct()
print("Union (Distinct) Results:")
union_df.show()

# Union all operation (include duplicates)
union_all_df = df1.union(df2)  # In newer versions of PySpark, use union() instead of unionAll()
print("Union All Results:")
union_all_df.show()

# Intersect operation
intersect_df = df1.intersect(df2)
print("Intersect Results:")
intersect_df.show()

# Perform Except operation
except_df = df1.exceptAll(df2)
print("Except Operation Results:")
except_df.show()

# Log information after displaying results
glueContext.get_logger().info("Results successfully displayed in the console for union, union all, and intersect operations.")

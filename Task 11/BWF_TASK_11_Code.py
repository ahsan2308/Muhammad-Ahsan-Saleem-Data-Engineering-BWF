# Import PySpark
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, hour, col, to_date, lower, substring
from pyspark.sql.types import TimestampType

# Create a SparkSession
spark = SparkSession.builder \
    .appName("BWF-PySparkTask") \
    .getOrCreate()


# Path to your CSV file
file_path = r'data.csv'

# Read CSV into DataFrame
orders_data = spark.read.csv(file_path, header=True, inferSchema=True)

# Display the DataFrame with reordered columns
orders_data.show()


# Add time_of_day column based on the hour
orders_data = orders_data.withColumn(
    "time_of_day",
    when((hour(col("order_date")) >= 5) & (hour(col("order_date")) < 12), "morning")
    .when((hour(col("order_date")) >= 12) & (hour(col("order_date")) < 18), "afternoon")
    .when((hour(col("order_date")) >= 18) & (hour(col("order_date")) < 24), "evening")
    .otherwise("night")
)


# Reorder columns to place 'time_of_day' at index 1
columns = orders_data.columns
desired_index = 1

# Remove 'time_of_day' from columns list to avoid duplication
columns.remove('time_of_day')

# Insert 'time_of_day' at the desired index
new_columns = columns[:desired_index] + ['time_of_day'] + columns[desired_index:]


# Reorder the DataFrame columns
orders_data = orders_data.select(new_columns)

# Display the DataFrame with reordered columns
orders_data.show(truncate=False)


# Extract the hour from order_date and filter out rows where the hour is between 0 and 5 inclusive
orders_data = orders_data.filter((hour(col("order_date")) < 0) | (hour(col("order_date")) > 5))

orders_data.show(truncate=False)


# Convert order_date column from timestamp to date
orders_data = orders_data.withColumn("order_date", to_date(col("order_date")))

orders_data.show(truncate=False)


# Remove rows where the product column has the value "TV"
orders_data = orders_data.filter(col("product") != "TV")

# Show the final DataFrame
orders_data.show(truncate=False)


# Ensure all values in the purchase_state column are lowercase
orders_data = orders_data.withColumn("category", lower(col("category")))

# Show the final DataFrame
orders_data.show(truncate=False)


# State is the 7th and 8th last characters
orders_data = orders_data.withColumn(
    "state",
    substring("purchase_address", -8, 2)
)

# Show the final DataFrame
orders_data.show(truncate=False)



# Path to save the Parquet file
parquet_path = '/orders_data.parquet'

# Save DataFrame as Parquet
orders_data.write.mode('overwrite').parquet(parquet_path)
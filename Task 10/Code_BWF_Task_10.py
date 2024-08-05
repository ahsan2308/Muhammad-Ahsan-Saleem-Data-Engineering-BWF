from pyspark.sql import SparkSession

# Initialize the SparkSession
spark = SparkSession.builder.appName("SalesDataProcessing").getOrCreate()

# Read the CSV file
file_path = r'C:\Users\mahsa\Desktop\Data\Bytewise Fellowship\Daily Tasks\Month 1\Task 10\Resources\dataset.csv'
sales_data = spark.read.csv(file_path, header=True, inferSchema=True)

# Display the first few rows
sales_data.show(5)
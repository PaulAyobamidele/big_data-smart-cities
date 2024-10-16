from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col

# Initialize the Spark session
spark = (
    SparkSession.builder.appName("KafkaStreamingApp")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0")
    .getOrCreate()
)

# Set log level to WARN to reduce output noise
spark.sparkContext.setLogLevel("WARN")

# Define the Kafka source for streaming
kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "TestTopic")
    .option("startingOffsets", "latest")  # Start reading from the latest messages
    .load()
)

# Select the value column and cast it to a string
lines_df = kafka_df.selectExpr("CAST(value AS STRING)")

# Split the lines into words
words_df = lines_df.select(explode(split(col("value"), " ")).alias("word"))

# Generate word count
word_count_df = words_df.groupBy("word").count()

# Output the results to the console
query = (
    word_count_df.writeStream.outputMode("update")
    .format("console")
    .option("truncate", "false")
    .start()
)

query.awaitTermination()

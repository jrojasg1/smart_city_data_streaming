from config import configuration
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from pyspark.sql.functions import from_json, col
from pyspark.sql import SparkSession, DataFrame

# Centralized schema definitions
schemas = {
    "vehicle_data": StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("make", StringType(), True),
        StructField("model", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("fuelType", StringType(), True)
    ]),
    "gps_data": StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("vehicleType", StringType(), True)
    ]),
    "traffic_data": StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("cameraId", StringType(), True),
        StructField("snapshot", StringType(), True)
    ]),
    "weather_data": StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("weatherCondition", StringType(), True),
        StructField("precipitation", IntegerType(), True),
        StructField("windSpeed", IntegerType(), True),
        StructField("humedity", IntegerType(), True),
        StructField("airQualityIndex", IntegerType(), True)
    ]),
    "emergency_data": StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("incidentId", StringType(), True),
        StructField("type", StringType(), True),
        StructField("status", StringType(), True),
        StructField("description", StringType(), True)
    ])
}

def configure_spark() -> SparkSession:
    """Configures and returns a SparkSession."""
    return SparkSession.builder.appName("SmartCityStreaming")\
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0," +
                "org.apache.hadoop:hadoop-aws:3.3.1," +
                "com.amazonaws:aws-java-sdk:1.11.469")\
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
        .config("spark.hadoop.fs.s3a.access.key", configuration.get('AWS_ACCESS_KEY'))\
        .config("spark.hadoop.fs.s3a.secret.key", configuration.get('AWS_SECRET_KEY'))\
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")\
        .getOrCreate()

def read_kafka_topic(spark: SparkSession, topic: str, schema: StructType) -> DataFrame:
    """Reads a Kafka topic and returns a DataFrame."""
    return (spark.readStream
            .format('kafka')
            .option('kafka.bootstrap.servers', 'broker:29092')
            .option('subscribe', topic)
            .option('startingOffsets', 'earliest')
            .load()
            .selectExpr('CAST(value AS STRING)')
            .select(from_json(col('value'), schema).alias('data'))
            .select('data.*')
            .withWatermark('timestamp', '2 minutes'))

def stream_writer(input_df: DataFrame, checkpoint_folder: str, output_folder: str):
    """Writes a streaming DataFrame to a specified output."""
    return (input_df.writeStream
            .format('parquet')
            .option('checkpointLocation', checkpoint_folder)
            .option('path', output_folder)
            .outputMode('append')
            .start())

def main():
    spark = configure_spark()
    spark.sparkContext.setLogLevel('WARN')

    # Kafka topics and output paths
    topics = {
        "vehicle_data": "s3a://spark-streaming-data-sm/data/vehicle_data",
        "gps_data": "s3a://spark-streaming-data-sm/data/gps_data",
        "traffic_data": "s3a://spark-streaming-data-sm/data/traffic_data",
        "weather_data": "s3a://spark-streaming-data-sm/data/weather_data",
        "emergency_data": "s3a://spark-streaming-data-sm/data/emergency_data"
    }

    queries = []

    for topic, output_path in topics.items():
        schema = schemas[topic]
        checkpoint_folder = f"s3a://spark-streaming-data-sm/checkpoints/{topic}"
        df = read_kafka_topic(spark, topic, schema)
        query = stream_writer(df, checkpoint_folder, output_path)
        queries.append(query)

    # Await termination for all queries
    for query in queries:
        query.awaitTermination()

if __name__ == "__main__":
    main()

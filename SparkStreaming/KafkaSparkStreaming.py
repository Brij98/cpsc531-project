from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType
from pyspark.sql.functions import *


KAFKA_TOPIC_NAME = "Read_heart_data"
KAFKA_BOOTSTRAP_SERVER = "localhost:9092"

if __name__ == "__main__":

    # creating spark session object
    spark = (
        SparkSession.builder.master("local[*]").
        appName("KafkaSparkStreaming").
        getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # read data stream from kafka topic
    dataFrame = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER)
        .option("subscribe", KAFKA_TOPIC_NAME)
        .option("startingOffsets", "latest")
        .load()
    )

    baseDataFrame = dataFrame.selectExpr("CAST(value as STRING)", "timestamp")
    baseDataFrame.printSchema()

    # applying the data schema
    inputDataSchema = (
        StructType()
        .add("age", IntegerType())
        .add("sex", BooleanType())
        .add("cp", IntegerType())
        .add("trestbps", IntegerType())
        .add("chol", IntegerType())
        .add("fbs", IntegerType())
        .add("restecg", IntegerType())
        .add("thalach", IntegerType())
        .add("exang", IntegerType())
        .add("oldpeak", FloatType())
        .add("slope", IntegerType())
        .add("ca", IntegerType())
        .add("thal", IntegerType())
        # .add("target", IntegerType())
    )

    info_dataframe = baseDataFrame.select(
        from_json(col("value"), inputDataSchema).alias("sample"), "timestamp"
    )
    info_df_fin = info_dataframe.select("sample.*", "timestamp")

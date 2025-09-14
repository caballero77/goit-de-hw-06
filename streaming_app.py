import findspark
findspark.init()
import json
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from config import USER_ID, kafka_config
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'

class AlertStreamingApp:
    def __init__(self):
        self.user_id = USER_ID
        self.input_topic = f"building_sensors_{USER_ID}"
        self.output_topic = f"alerts_{USER_ID}"
        
        self.spark = (SparkSession.builder
            .appName("AlertStreamingApp")
            .master("local[*]")
            .config("spark.sql.streaming.stateStore.stateSchemaCheck", "false")
            .getOrCreate())
        
        self.spark.sparkContext.setLogLevel("ERROR")
        
        logger.info(f"Spark session initialized")
        logger.info(f"Input topic: {self.input_topic}")
        logger.info(f"Output topic: {self.output_topic}")
        
        self.alert_conditions = self.load_alert_conditions()
        
    def load_alert_conditions(self):
        try:
            df = pd.read_csv('alerts_conditions.csv')
            logger.info(f"Loaded {len(df)} alert conditions")
            logger.info(f"Alert conditions:\n{df}")
            return df
        except Exception as e:
            logger.error(f"Error loading alert conditions: {e}")
            return None
    
    def create_kafka_stream(self):
        kafka_options = {
            "kafka.bootstrap.servers": ",".join(kafka_config['bootstrap_servers']),
            "kafka.security.protocol": kafka_config['security_protocol'],
            "kafka.sasl.mechanism": kafka_config['sasl_mechanism'],
            "kafka.sasl.jaas.config": f"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{kafka_config['username']}\" password=\"{kafka_config['password']}\";",
            "subscribe": self.input_topic,
            "startingOffsets": "earliest"
        }
        
        stream = (self.spark
            .readStream
            .format("kafka")
            .options(**kafka_options)
            .load())
        
        return stream
    
    def parse_sensor_data(self, stream):
        sensor_schema = StructType([
            StructField("sensor_id", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("temperature", DoubleType(), True),
            StructField("humidity", DoubleType(), True),
            StructField("building_id", StringType(), True),
            StructField("floor", IntegerType(), True)
        ])
        
        parsed_stream = (stream
            .select(
                from_json(col("value").cast("string"), sensor_schema).alias("data"),
                col("timestamp").alias("kafka_timestamp")
            )
            .select(
                col("data.*"),
                col("kafka_timestamp")
            )
            .withColumn("timestamp", to_timestamp(col("timestamp")))
            .withColumn("id", col("sensor_id"))
        )
        return parsed_stream
    
    def aggregate_data(self, parsed_stream):
        windowed_stream = (parsed_stream 
            .withWatermark("timestamp", "10 seconds")
            .groupBy(
                window(col("timestamp"), "1 minute", "30 seconds"),
                col("building_id"),
                col("floor")
            )
            .agg(
                avg("temperature").alias("t_avg"),
                avg("humidity").alias("h_avg"),
                count("*").alias("count")
            )
            .filter(col("count") > 0))
        return windowed_stream
    
    def create_alert_conditions_df(self):
        if self.alert_conditions is None:
            return None
            
        alert_df = self.spark.createDataFrame(self.alert_conditions)
        
        alert_df = alert_df.withColumn("dummy", lit(1))
        
        return alert_df
    
    def detect_alerts(self, aggregated_stream):
        alert_conditions_df = self.create_alert_conditions_df()
        
        if alert_conditions_df is None:
            logger.error("No alert conditions available")
            return None
        
        aggregated_stream = aggregated_stream.withColumn("dummy", lit(1))
        
        alerts_stream = (aggregated_stream
            .crossJoin(alert_conditions_df)
            .drop("dummy") \
            .filter(
                (
                    (col("temperature_min") != -999) & (col("temperature_max") != -999) &
                    (
                        (col("t_avg") < col("temperature_max")) & (col("t_avg") > col("temperature_min"))
                    )
                ) |
                (
                    (col("humidity_min") != -999) & (col("humidity_max") != -999) &
                    (
                        (col("h_avg") < col("humidity_max")) & (col("h_avg") > col("humidity_min"))
                    )
                )
            )
            .select(
                col("window"),
                col("building_id"),
                col("floor"),
                col("t_avg"),
                col("h_avg"),
                col("code"),
                col("message"),
                col("window.start").alias("timestamp")
            ))
        
        return alerts_stream
    
    def write_to_kafka(self, alerts_stream):
        kafka_options = {
            "kafka.bootstrap.servers": ",".join(kafka_config['bootstrap_servers']),
            "kafka.security.protocol": kafka_config['security_protocol'],
            "kafka.sasl.mechanism": kafka_config['sasl_mechanism'],
            "kafka.sasl.jaas.config": f"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{kafka_config['username']}\" password=\"{kafka_config['password']}\";",
            "topic": self.output_topic
        }
        
        json_alerts = alerts_stream \
            .select(
                to_json(struct(
                    col("window"),
                    col("building_id"),
                    col("floor"),
                    col("t_avg"),
                    col("h_avg"),
                    col("code"),
                    col("message"),
                    col("timestamp")
                )).alias("value")
            )
        
        query = (json_alerts
            .writeStream
            .format("kafka")
            .options(**kafka_options)
            .option("checkpointLocation", "./checkpoint")
            .outputMode("append")
            .start())
        
        return query
    
    def run(self):
        try:
            logger.info("Starting streaming application...")
            
            kafka_stream = self.create_kafka_stream()
            
            parsed_stream = self.parse_sensor_data(kafka_stream)
            
            aggregated_stream = self.aggregate_data(parsed_stream)
            
            alerts_stream = self.detect_alerts(aggregated_stream)
            
            if alerts_stream is None:
                logger.error("Failed to create alerts stream")
                return
            
            query = self.write_to_kafka(alerts_stream)
            
            logger.info("Streaming application started successfully")
            logger.info("Press Ctrl+C to stop...")
            
            query.awaitTermination()
            
        except KeyboardInterrupt:
            logger.info("Application stopped by user")
        except Exception as e:
            logger.error(f"Error in streaming application: {e}")
        finally:
            self.spark.stop()
            logger.info("Spark session stopped")


if __name__ == "__main__":
    app = AlertStreamingApp()
    app.run()
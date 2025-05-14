import pika
import time
import logging
import json
from datetime import datetime, timezone
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# InfluxDB configuration
INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://influxdb:8086")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN", "mytoken")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "myorg")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "test_1")

# Initialize InfluxDB client
client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
write_api = client.write_api(write_options=SYNCHRONOUS)

def callback(ch, method, properties, body):
    try:
        # Decodificar el mensaje JSON
        message = json.loads(body)
        logger.info(f"Received JSON message: {message}")
        
        # Guardar en InfluxDB
        point = Point("impressions") \
            .tag("impression_id", message["impression_id"]) \
            .tag("session_id", message["session_id"]) \
            .tag("state", message["state"]) \
            .field("user_ip", message["user_ip"]) \
            .field("user_agent", message["user_agent"]) \
            .field("search_keywords", message["search_keywords"])
        
        # Usar el timestamp del mensaje
        timestamp = datetime.fromisoformat(message["timestamp"].replace('Z', '+00:00'))
        point = point.time(timestamp)
        
        logger.info(f"Saving point to InfluxDB: {point.to_line_protocol()}")
        write_api.write(bucket=INFLUXDB_BUCKET, record=point)
        logger.info("Successfully saved to InfluxDB")
        
        time.sleep(1)
        logger.info("Done processing")
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON message: {e}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")

def consume():
    logger.info("Starting consumer")
    time.sleep(5)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='rabbitmq')
    )

    channel = connection.channel()
    channel.queue_declare(queue='demo_queue')
    channel.basic_consume(queue='demo_queue', on_message_callback=callback, auto_ack=True)
    logger.info("Waiting for messages...")
    channel.start_consuming()

if __name__ == "__main__":
    consume()
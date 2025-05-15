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

def process_impression(message):
    point = Point("impressions") \
        .tag("impression_id", message["impression_id"]) \
        .tag("session_id", message["session_id"]) \
        .tag("state", message["state"]) \
        .field("user_ip", message["user_ip"]) \
        .field("user_agent", message["user_agent"]) \
        .field("search_keywords", message["search_keywords"]) \
        .field("ads", json.dumps(message["ads"]))
    
    timestamp = datetime.fromisoformat(message["timestamp"].replace('Z', '+00:00'))
    point = point.time(timestamp)
    return point

def process_click(message):
    point = Point("clicks") \
        .tag("click_id", message["click_id"]) \
        .tag("impression_id", message["impression_id"]) \
        .field("clicked_ad", json.dumps(message["clicked_ad"])) \
        .field("user_info", json.dumps(message["user_info"]))
    
    timestamp = datetime.fromisoformat(message["timestamp"].replace('Z', '+00:00'))
    point = point.time(timestamp)
    return point

def process_conversion(message):
    point = Point("conversions") \
        .tag("conversion_id", message["conversion_id"]) \
        .tag("click_id", message["click_id"]) \
        .tag("impression_id", message["impression_id"]) \
        .field("conversion_type", message["conversion_type"]) \
        .field("conversion_value", message["conversion_value"]) \
        .field("conversion_currency", message["conversion_currency"]) \
        .field("conversion_attributes", json.dumps(message["conversion_attributes"])) \
        .field("attribution_info", json.dumps(message["attribution_info"])) \
        .field("user_info", json.dumps(message["user_info"]))
    
    timestamp = datetime.fromisoformat(message["timestamp"].replace('Z', '+00:00'))
    point = point.time(timestamp)
    return point

def callback(ch, method, properties, body):
    try:
        # Decodificar el mensaje JSON
        message = json.loads(body)
        logger.info(f"Received JSON message from queue {method.routing_key}")
        
        # Procesar según el tipo de mensaje
        if method.routing_key == "Ad Impression":
            point = process_impression(message)
        elif method.routing_key == "Ad Click":
            point = process_click(message)
        elif method.routing_key == "Ad Conversion":
            point = process_conversion(message)
        else:
            logger.error(f"Unknown routing key: {method.routing_key}")
            return
        
        logger.info(f"Saving point to InfluxDB: {point.to_line_protocol()}")
        write_api.write(bucket=INFLUXDB_BUCKET, record=point)
        logger.info("Successfully saved to InfluxDB")
        
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON message: {e}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")

def consume():
    logger.info("Starting consumer")
    time.sleep(5)  # Esperar a que RabbitMQ esté listo
    
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='rabbitmq')
    )
    channel = connection.channel()
    
    # Declarar las tres colas
    channel.queue_declare(queue='Ad Impression')
    channel.queue_declare(queue='Ad Click')
    channel.queue_declare(queue='Ad Conversion')
    
    # Configurar los consumidores para cada cola
    channel.basic_consume(queue='Ad Impression', on_message_callback=callback, auto_ack=True)
    channel.basic_consume(queue='Ad Click', on_message_callback=callback, auto_ack=True)
    channel.basic_consume(queue='Ad Conversion', on_message_callback=callback, auto_ack=True)
    
    logger.info("Waiting for messages...")
    channel.start_consuming()

if __name__ == "__main__":
    consume()
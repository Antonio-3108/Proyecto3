import pika
from fastapi import FastAPI, Body, HTTPException
from datetime import datetime, timezone
import os
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from typing import Dict, List, Any
import json
import logging
from pydantic import BaseModel

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

# InfluxDB configuration
INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://influxdb:8086")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN", "")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "myorg")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "Ad Impression")

logger.info(f"InfluxDB Configuration - URL: {INFLUXDB_URL}, Org: {INFLUXDB_ORG}, Bucket: {INFLUXDB_BUCKET}")
if not INFLUXDB_TOKEN:
    logger.error("INFLUXDB_TOKEN no está configurado. Por favor, configura la variable de entorno INFLUXDB_TOKEN con tu token de InfluxDB.")
    raise ValueError("INFLUXDB_TOKEN no está configurado")

# Initialize InfluxDB client
try:
    client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
    # Verify connection
    health = client.health()
    logger.info(f"InfluxDB Health Check: {health}")
    
    # Verify bucket exists and create if it doesn't
    buckets_api = client.buckets_api()
    try:
        bucket = buckets_api.find_bucket_by_name(INFLUXDB_BUCKET)
        if bucket is None:
            logger.info(f"Bucket {INFLUXDB_BUCKET} does not exist, creating it...")
            bucket = buckets_api.create_bucket(bucket_name=INFLUXDB_BUCKET, org=INFLUXDB_ORG)
            logger.info(f"Created bucket: {bucket.name}")
        else:
            logger.info(f"Found bucket: {bucket.name}")
    except Exception as e:
        logger.error(f"Error managing bucket: {str(e)}")
        raise
    
    write_api = client.write_api(write_options=SYNCHRONOUS)
except Exception as e:
    logger.error(f"Error initializing InfluxDB client: {str(e)}")
    raise

# Modelos para la estructura de anuncios
class Advertiser(BaseModel):
    advertiser_id: str
    advertiser_name: str

class Campaign(BaseModel):
    campaign_id: str
    campaign_name: str

class Ad(BaseModel):
    ad_id: str
    ad_name: str
    ad_text: str
    ad_link: str
    ad_position: int
    ad_format: str

class AdInfo(BaseModel):
    advertiser: Advertiser
    campaign: Campaign
    ad: Ad

class ImpressionPayload(BaseModel):
    impression_id: str
    user_ip: str
    user_agent: str
    timestamp: datetime
    state: str
    search_keywords: str
    session_id: str
    ads: List[AdInfo]

app = FastAPI()

@app.post("/send")
async def send_message(payload: ImpressionPayload):
    try:
        # Conexión a RabbitMQ
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rabbitmq')
        )
        channel = connection.channel()
        channel.queue_declare(queue='Ad Impression')
        
        # Convertir el payload a JSON string usando el encoder personalizado
        message_content = json.dumps(payload.dict(), cls=DateTimeEncoder)
        logger.info(f"Processing message: {message_content}")
        
        # Enviar mensaje a RabbitMQ
        channel.basic_publish(exchange='', routing_key='Ad Impression', body=message_content)
        connection.close()
        
        # Guardar en InfluxDB
        point = Point("Ad Impression") \
            .field("impression_id", payload.impression_id) \
            .field("user_ip", payload.user_ip) \
            .field("user_agent", payload.user_agent) \
            .field("state", payload.state) \
            .field("search_keywords", payload.search_keywords) \
            .field("session_id", payload.session_id) \
            .field("timestamp", payload.timestamp.isoformat()) \
            .field("ads", json.dumps([ad.dict() for ad in payload.ads], cls=DateTimeEncoder))
        
        write_api.write(bucket=INFLUXDB_BUCKET, record=point)
        
        return {"message": "Message sent to RabbitMQ and stored in InfluxDB", "payload": payload.dict()}
    except Exception as e:
        logger.error(f"Error processing message: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error processing message: {str(e)}")

@app.get("/messages")
def get_messages():
    query = f'''
    from(bucket:"{INFLUXDB_BUCKET}")
        |> range(start: -1h)
        |> filter(fn: (r) => r["_measurement"] == "Ad Impression")
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        |> sort(columns: ["_time"], desc: true)
        |> limit(n: 1)
    '''
    result = client.query_api().query(query=query, org=INFLUXDB_ORG)
    
    messages = []
    for table in result:
        for record in table.records:
            message = {
                "impression_id": record.values.get("impression_id"),
                "user_ip": record.values.get("user_ip"),
                "user_agent": record.values.get("user_agent"),
                "state": record.values.get("state"),
                "search_keywords": record.values.get("search_keywords"),
                "session_id": record.values.get("session_id"),
                "timestamp": record.values.get("timestamp"),
                "ads": json.loads(record.values.get("ads", "[]"))
            }
            messages.append(message)
    
    return messages

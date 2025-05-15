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
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "test_1")

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

class click_coordinates(BaseModel):
    x: int
    y: int
    normalized_x: float
    normalized_y: float
class clicked_ad(BaseModel):
    ad_id: str
    ad_position: int
    click_coordinates: click_coordinates
    time_to_click: int
class user_info(BaseModel):
    user_ip: str
    state: str
    session_id: str

class item(BaseModel):
    product_id: str
    quantity: int
    unit_price: float
class conversion_attributes(BaseModel):
    order_id: str
    items: List[item]
class conversion_path(BaseModel):
    event_type: str
    timestamp: datetime
class attribution_info(BaseModel):
    time_to_convert: int
    attribution_model: str
    conversion_path: List[conversion_path]

class ImpressionPayload(BaseModel):
    impression_id: str
    user_ip: str
    user_agent: str
    timestamp: datetime
    state: str
    search_keywords: str
    session_id: str
    ads: List[AdInfo]

class clickPayload(BaseModel):
    click_id: str
    impression_id: str
    timestamp: datetime
    clicked_ad: clicked_ad
    user_info: user_info

class conversionPayload(BaseModel):
    conversion_id: str
    click_id: str
    impression_id: str
    timestamp: datetime
    conversion_type: str
    conversion_value: float
    conversion_currency: str
    conversion_attributes: conversion_attributes
    attribution_info: attribution_info
    user_info: user_info

app = FastAPI()

@app.post("/api/events/impression")
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
    
@app.post("/api/events/click")
async def send_message(payload: clickPayload):
    try:
        # Conexión a RabbitMQ
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rabbitmq')
        )
        channel = connection.channel()
        channel.queue_declare(queue='Ad Click')
        
        # Convertir el payload a JSON string usando el encoder personalizado
        message_content = json.dumps(payload.dict(), cls=DateTimeEncoder)
        logger.info(f"Processing message: {message_content}")
        
        # Enviar mensaje a RabbitMQ
        channel.basic_publish(exchange='', routing_key='Ad Click', body=message_content)
        connection.close()
        
        # Guardar en InfluxDB
        point = Point("Ad Click") \
            .field("click_id", payload.click_id) \
            .field("impression_id", payload.impression_id) \
            .field("timestamp", payload.timestamp.isoformat()) \
            .field("clicked_ad", json.dumps(payload.clicked_ad.dict(), cls=DateTimeEncoder)) \
            .field("user_info", json.dumps(payload.user_info.dict(), cls=DateTimeEncoder))
        
        write_api.write(bucket=INFLUXDB_BUCKET, record=point)
        
        return {"message": "Message sent to RabbitMQ and stored in InfluxDB", "payload": payload.dict()}
    except Exception as e:
        logger.error(f"Error processing message: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error processing message: {str(e)}")
    
@app.post("/api/events/conversion")
async def send_message(payload: conversionPayload):
    try:
        # Conexión a RabbitMQ
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rabbitmq')
        )
        channel = connection.channel()
        channel.queue_declare(queue='Ad Conversion')
        
        # Convertir el payload a JSON string usando el encoder personalizado
        message_content = json.dumps(payload.dict(), cls=DateTimeEncoder)
        logger.info(f"Processing message: {message_content}")
        
        # Enviar mensaje a RabbitMQ
        channel.basic_publish(exchange='', routing_key='Ad Conversion', body=message_content)
        connection.close()
        
        # Guardar en InfluxDB
        point = Point("Ad Conversion") \
            .field("conversion_id", payload.conversion_id) \
            .field("click_id", payload.click_id) \
            .field("impression_id", payload.impression_id) \
            .field("timestamp", payload.timestamp.isoformat()) \
            .field("conversion_type", payload.conversion_type) \
            .field("conversion_value", payload.conversion_value) \
            .field("conversion_currency", payload.conversion_currency) \
            .field("conversion_attributes", json.dumps(payload.conversion_attributes.dict(), cls=DateTimeEncoder)) \
            .field("attribution_info", json.dumps(payload.attribution_info.dict(), cls=DateTimeEncoder)) \
            .field("user_info", json.dumps(payload.user_info.dict(), cls=DateTimeEncoder))
        
        write_api.write(bucket=INFLUXDB_BUCKET, record=point)
        
        return {"message": "Message sent to RabbitMQ and stored in InfluxDB", "payload": payload.dict()}
    except Exception as e:
        logger.error(f"Error processing message: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error processing message: {str(e)}")
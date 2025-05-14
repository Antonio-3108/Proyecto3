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
import uuid

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
        
        # Guardar cada anuncio en la tabla AdInfo y recolectar sus uids
        ad_uids = []
        for ad_info in payload.ads:
            ad_uid = str(uuid.uuid4())
            ad_point = Point("AdInfo") \
                .tag("uid", ad_uid) \
                .tag("impression_id", payload.impression_id) \
                .field("advertiser_id", ad_info.advertiser.advertiser_id) \
                .field("advertiser_name", ad_info.advertiser.advertiser_name) \
                .field("campaign_id", ad_info.campaign.campaign_id) \
                .field("campaign_name", ad_info.campaign.campaign_name) \
                .field("ad_id", ad_info.ad.ad_id) \
                .field("ad_name", ad_info.ad.ad_name) \
                .field("ad_text", ad_info.ad.ad_text) \
                .field("ad_link", ad_info.ad.ad_link) \
                .field("ad_position", ad_info.ad.ad_position) \
                .field("ad_format", ad_info.ad.ad_format) \
                .time(payload.timestamp)
            
            write_api.write(bucket=INFLUXDB_BUCKET, record=ad_point)
            ad_uids.append(ad_uid)
        
        # Guardar la impresión principal en InfluxDB con la lista de uids de anuncios
        impression_point = Point("Ad Impression") \
            .field("impression_id", payload.impression_id) \
            .field("user_ip", payload.user_ip) \
            .field("user_agent", payload.user_agent) \
            .field("state", payload.state) \
            .field("search_keywords", payload.search_keywords) \
            .field("session_id", payload.session_id) \
            .field("timestamp", payload.timestamp.isoformat()) \
            .field("ads", json.dumps(ad_uids))
        
        write_api.write(bucket=INFLUXDB_BUCKET, record=impression_point)
        
        return {"message": "Message sent to RabbitMQ and stored in InfluxDB", "payload": payload.dict()}
    except Exception as e:
        logger.error(f"Error processing message: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error processing message: {str(e)}")

@app.get("/messages")
def get_messages():
    # Primero obtenemos las impresiones
    impression_query = f'''
    from(bucket:"{INFLUXDB_BUCKET}")
        |> range(start: -1h)
        |> filter(fn: (r) => r["_measurement"] == "Ad Impression")
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        |> sort(columns: ["_time"], desc: true)
        |> limit(n: 1)
    '''
    impression_result = client.query_api().query(query=impression_query, org=INFLUXDB_ORG)
    
    messages = []
    for table in impression_result:
        for record in table.records:
            impression_id = record.values.get("impression_id")
            ad_uids = json.loads(record.values.get("ads", "[]"))
            logger.info(f"Found impression {impression_id} with ad_uids: {ad_uids}")
            
            # Obtener los anuncios asociados usando los uids
            ads = []
            for ad_uid in ad_uids:
                logger.info(f"Querying for ad with uid: {ad_uid}")
                ad_query = f'''
                from(bucket:"{INFLUXDB_BUCKET}")
                    |> range(start: -1h)
                    |> filter(fn: (r) => r["_measurement"] == "AdInfo")
                    |> filter(fn: (r) => r["uid"] == "{ad_uid}")
                    |> filter(fn: (r) => r["_field"] != "uid" and r["_field"] != "impression_id")
                    |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                '''
                ad_result = client.query_api().query(query=ad_query, org=INFLUXDB_ORG)
                logger.info(f"Query result for ad {ad_uid}: {ad_result}")
                
                for ad_table in ad_result:
                    for ad_record in ad_table.records:
                        ad = {
                            "advertiser": {
                                "advertiser_id": ad_record.values.get("advertiser_id"),
                                "advertiser_name": ad_record.values.get("advertiser_name")
                            },
                            "campaign": {
                                "campaign_id": ad_record.values.get("campaign_id"),
                                "campaign_name": ad_record.values.get("campaign_name")
                            },
                            "ad": {
                                "ad_id": ad_record.values.get("ad_id"),
                                "ad_name": ad_record.values.get("ad_name"),
                                "ad_text": ad_record.values.get("ad_text"),
                                "ad_link": ad_record.values.get("ad_link"),
                                "ad_position": ad_record.values.get("ad_position"),
                                "ad_format": ad_record.values.get("ad_format")
                            }
                        }
                        ads.append(ad)
            
            message = {
                "impression_id": impression_id,
                "user_ip": record.values.get("user_ip"),
                "user_agent": record.values.get("user_agent"),
                "state": record.values.get("state"),
                "search_keywords": record.values.get("search_keywords"),
                "session_id": record.values.get("session_id"),
                "timestamp": record.values.get("timestamp"),
                "ads": ads
            }
            messages.append(message)
    
    return messages

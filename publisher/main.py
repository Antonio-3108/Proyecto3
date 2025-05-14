import pika
from fastapi import FastAPI, Body
from datetime import datetime, timezone
import os
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from typing import Dict, List, Any
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# InfluxDB configuration
INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://influxdb:8086")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN", "")  # Dejar vacío para forzar el uso de la variable de entorno
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "myorg")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "mybucket")

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

app = FastAPI()

@app.post("/send")
def send_message():
    # Conexión a RabbitMQ
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='rabbitmq')
    )
    channel = connection.channel()
    channel.queue_declare(queue='demo_queue')
    
    message_content = 'Hello World!'
    
    # Enviar mensaje a RabbitMQ
    channel.basic_publish(exchange='', routing_key='demo_queue', body=message_content)
    connection.close()
    
    # Guardar en InfluxDB
    point = Point("messages") \
        .field("content", message_content) \
        .time(datetime.now(timezone.utc))
    
    write_api.write(bucket=INFLUXDB_BUCKET, record=point)
    
    return {"message": "Message sent to RabbitMQ and stored in InfluxDB"}

# Endpoint para obtener todos los mensajes
@app.get("/messages")
def get_messages():
    query = f'from(bucket:"{INFLUXDB_BUCKET}") |> range(start: -1h)'
    result = client.query_api().query(query=query, org=INFLUXDB_ORG)
    
    messages = []
    for table in result:
        for record in table.records:
            messages.append({
                "content": record.get_value(),
                "time": record.get_time()
            })
    
    return messages

# Nuevo endpoint para procesar impresiones de anuncios
@app.post("/ad-impressions")
def process_ad_impression(data: Dict[str, Any] = Body(...)):
    try:
        logger.info(f"Processing new ad impression with ID: {data.get('impression_id')}")
        
        # 1. Almacenar el evento principal de impresión en InfluxDB
        timestamp = data.get("timestamp")
        if timestamp:
            parsed_time = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        else:
            parsed_time = datetime.now(timezone.utc)
            
        logger.info(f"Creating impression point with timestamp: {parsed_time}")
        impression_point = Point("ad_impressions") \
            .tag("impression_id", data.get("impression_id", "")) \
            .tag("state", data.get("state", "")) \
            .tag("session_id", data.get("session_id", "")) \
            .field("user_ip", data.get("user_ip", "")) \
            .field("user_agent", data.get("user_agent", "")) \
            .field("search_keywords", data.get("search_keywords", "")) \
            .time(parsed_time)
        
        try:
            logger.info(f"Writing impression point to InfluxDB bucket: {INFLUXDB_BUCKET}")
            write_api.write(bucket=INFLUXDB_BUCKET, record=impression_point)
            logger.info("Successfully wrote impression point")
            
            # Verify the write by reading it back
            query = f'from(bucket:"{INFLUXDB_BUCKET}") |> range(start: -1h) |> filter(fn: (r) => r._measurement == "ad_impressions" and r.impression_id == "{data.get("impression_id")}")'
            result = client.query_api().query(query=query, org=INFLUXDB_ORG)
            if len(result) > 0:
                logger.info("Verified impression point was written successfully")
            else:
                logger.warning("Could not verify impression point was written")
        except Exception as e:
            logger.error(f"Error writing impression point to InfluxDB: {str(e)}")
            raise
        
        # 2. Almacenar información detallada de cada anuncio
        logger.info(f"Processing {len(data.get('ads', []))} ads")
        for ad_data in data.get("ads", []):
            try:
                # Crear un punto para cada anuncio mostrado
                logger.info(f"Creating ad point for ad_id: {ad_data.get('ad', {}).get('ad_id')}")
                ad_point = Point("ad_details") \
                    .tag("impression_id", data.get("impression_id", "")) \
                    .tag("advertiser_id", ad_data.get("advertiser", {}).get("advertiser_id", "")) \
                    .tag("advertiser_name", ad_data.get("advertiser", {}).get("advertiser_name", "")) \
                    .tag("campaign_id", ad_data.get("campaign", {}).get("campaign_id", "")) \
                    .tag("campaign_name", ad_data.get("campaign", {}).get("campaign_name", "")) \
                    .tag("ad_id", ad_data.get("ad", {}).get("ad_id", "")) \
                    .tag("ad_name", ad_data.get("ad", {}).get("ad_name", "")) \
                    .field("ad_text", ad_data.get("ad", {}).get("ad_text", "")) \
                    .field("ad_link", ad_data.get("ad", {}).get("ad_link", "")) \
                    .field("ad_position", ad_data.get("ad", {}).get("ad_position", 0)) \
                    .field("ad_format", ad_data.get("ad", {}).get("ad_format", "")) \
                    .time(parsed_time)
                
                logger.info(f"Writing ad point to InfluxDB bucket: {INFLUXDB_BUCKET}")
                write_api.write(bucket=INFLUXDB_BUCKET, record=ad_point)
                logger.info("Successfully wrote ad point")
                
                # Verify the write by reading it back
                query = f'from(bucket:"{INFLUXDB_BUCKET}") |> range(start: -1h) |> filter(fn: (r) => r._measurement == "ad_details" and r.impression_id == "{data.get("impression_id")}" and r.ad_id == "{ad_data.get("ad", {}).get("ad_id")}")'
                result = client.query_api().query(query=query, org=INFLUXDB_ORG)
                if len(result) > 0:
                    logger.info("Verified ad point was written successfully")
                else:
                    logger.warning("Could not verify ad point was written")
            except Exception as e:
                logger.error(f"Error writing ad point to InfluxDB: {str(e)}")
                raise

        # 3. Enviar a RabbitMQ para procesamiento asincrónico
        logger.info("Connecting to RabbitMQ")
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rabbitmq')
        )
        channel = connection.channel()
        channel.queue_declare(queue='ad_impressions_queue')
        
        # Convertir a JSON string para enviar a RabbitMQ
        message_body = json.dumps(data)
        logger.info("Publishing message to RabbitMQ")
        channel.basic_publish(
            exchange='',
            routing_key='ad_impressions_queue',
            body=message_body
        )
        connection.close()
        logger.info("Successfully published to RabbitMQ")
        
        return {
            "status": "success",
            "message": "Ad impression processed and stored",
            "impression_id": data.get("impression_id")
        }
        
    except Exception as e:
        logger.error(f"Error processing ad impression: {str(e)}")
        return {
            "status": "error",
            "message": f"Error processing ad impression: {str(e)}"
        }

# Endpoint para consultar impresiones de anuncios
@app.get("/ad-impressions")
def get_ad_impressions(hours: int = 24):
    try:
        logger.info(f"Querying ad impressions for the last {hours} hours")
        
        # Verificar conexión con InfluxDB
        try:
            health = client.health()
            logger.info(f"InfluxDB Health Check: {health}")
        except Exception as e:
            logger.error(f"Error checking InfluxDB health: {str(e)}")
            raise
        
        # Consultar impresiones principales
        query_impressions = f'from(bucket:"{INFLUXDB_BUCKET}")' \
                          f' |> range(start: -{hours}h)' \
                          f' |> filter(fn: (r) => r._measurement == "ad_impressions")'
        
        logger.info(f"Executing query: {query_impressions}")
        try:
            result_impressions = client.query_api().query(query=query_impressions, org=INFLUXDB_ORG)
            logger.info(f"Found {len(result_impressions)} tables in impressions query")
        except Exception as e:
            logger.error(f"Error querying impressions: {str(e)}")
            raise
        
        impressions = {}
        for table in result_impressions:
            for record in table.records:
                impression_id = record.values.get("impression_id")
                if impression_id not in impressions:
                    impressions[impression_id] = {
                        "impression_id": impression_id,
                        "state": record.values.get("state"),
                        "session_id": record.values.get("session_id"),
                        "timestamp": record.get_time().isoformat(),
                        "ads": []
                    }
                    
                    if record.get_field() == "user_ip":
                        impressions[impression_id]["user_ip"] = record.get_value()
                    elif record.get_field() == "user_agent":
                        impressions[impression_id]["user_agent"] = record.get_value()
                    elif record.get_field() == "search_keywords":
                        impressions[impression_id]["search_keywords"] = record.get_value()
        
        logger.info(f"Processed {len(impressions)} impressions")
        
        # Consultar detalles de anuncios
        query_ads = f'from(bucket:"{INFLUXDB_BUCKET}")' \
                  f' |> range(start: -{hours}h)' \
                  f' |> filter(fn: (r) => r._measurement == "ad_details")'
        
        logger.info(f"Executing query: {query_ads}")
        try:
            result_ads = client.query_api().query(query=query_ads, org=INFLUXDB_ORG)
            logger.info(f"Found {len(result_ads)} tables in ads query")
        except Exception as e:
            logger.error(f"Error querying ads: {str(e)}")
            raise
        
        # Organizar los datos de anuncios por impression_id
        ad_details = {}
        for table in result_ads:
            for record in table.records:
                impression_id = record.values.get("impression_id")
                ad_id = record.values.get("ad_id")
                
                # Inicializar estructura si no existe
                if impression_id not in ad_details:
                    ad_details[impression_id] = {}
                
                if ad_id not in ad_details[impression_id]:
                    ad_details[impression_id][ad_id] = {
                        "advertiser": {
                            "advertiser_id": record.values.get("advertiser_id"),
                            "advertiser_name": record.values.get("advertiser_name")
                        },
                        "campaign": {
                            "campaign_id": record.values.get("campaign_id"),
                            "campaign_name": record.values.get("campaign_name")
                        },
                        "ad": {
                            "ad_id": ad_id,
                            "ad_name": record.values.get("ad_name")
                        }
                    }
                
                # Agregar campos específicos
                field = record.get_field()
                value = record.get_value()
                
                if field == "ad_text":
                    ad_details[impression_id][ad_id]["ad"]["ad_text"] = value
                elif field == "ad_link":
                    ad_details[impression_id][ad_id]["ad"]["ad_link"] = value
                elif field == "ad_position":
                    ad_details[impression_id][ad_id]["ad"]["ad_position"] = value
                elif field == "ad_format":
                    ad_details[impression_id][ad_id]["ad"]["ad_format"] = value
        
        logger.info(f"Processed {len(ad_details)} ad details")
        
        # Combinar datos
        for impression_id, ads in ad_details.items():
            if impression_id in impressions:
                impressions[impression_id]["ads"] = list(ads.values())
        
        result = list(impressions.values())
        logger.info(f"Returning {len(result)} impressions with their ads")
        return result
        
    except Exception as e:
        logger.error(f"Error in get_ad_impressions: {str(e)}")
        raise
import pika
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def callback(ch, method, properties, body):
    logger.info(f"Received {body}")
    time.sleep(1)
    logger.info("Done processing")

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
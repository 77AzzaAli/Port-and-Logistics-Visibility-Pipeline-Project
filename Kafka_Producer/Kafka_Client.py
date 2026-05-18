import json
import time
from kafka import KafkaProducer

from Config import KAFKA_BROKER
from logger import logger


def create_producer():
    """
    Creates Kafka producer with retry backoff until Kafka is available.
    """

    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                acks="all",
                retries=5,
                enable_idempotence=True,
                linger_ms=10,
                max_block_ms=5000
            )

            logger.info("Kafka Producer Connected Successfully")
            return producer

        except Exception as e:
            logger.error(f"Kafka connection failed: {e}")
            logger.info("Retrying in 5 seconds...")
            time.sleep(5)
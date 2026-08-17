"""Consumer Application for Google Cloud Managed Service for Apache Kafka POC.

Subscribes to Kafka topic, tracks consumer group offsets, logs received payloads,
and exposes health and metric endpoints for Cloud Run.
"""

import asyncio
import json
import logging
import os
import sys
import threading
import time
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional

from fastapi import FastAPI
import uvicorn

# Setup paths for resilient imports
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
for p in [current_dir, parent_dir]:
    if p not in sys.path:
        sys.path.insert(0, p)

try:
    from auth.gcp_token_provider import confluent_oauth_callback
except ImportError:
    try:
        from gcp_token_provider import confluent_oauth_callback
    except ImportError:
        confluent_oauth_callback = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("kafka-consumer-app")

# Configuration via Environment Variables
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "orders-poc")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "order-processing-group")
AUTO_OFFSET_RESET = os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")
AUTH_TYPE = os.getenv("KAFKA_AUTH_TYPE", "OAUTHBEARER").upper()
PORT = int(os.getenv("PORT", "8080"))

consumer_running = False
consumer_thread: Optional[threading.Thread] = None

stats = {
    "messages_consumed": 0,
    "last_consumed_message": None,
    "last_error": None,
    "is_consuming": False,
    "offsets_by_partition": {},
    "bootstrap_servers": BOOTSTRAP_SERVERS,
    "topic": TOPIC_NAME,
    "group_id": GROUP_ID,
    "auth_type": AUTH_TYPE,
}


def create_kafka_consumer():
    """Initializes confluent_kafka Consumer instance with appropriate auth."""
    from confluent_kafka import Consumer

    config = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": AUTO_OFFSET_RESET,
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 1000,
        "session.timeout.ms": 30000,
        "socket.timeout.ms": 10000,
    }

    if AUTH_TYPE == "OAUTHBEARER":
        logger.info("Configuring SASL_SSL OAUTHBEARER consumer with GCP ADC...")
        config.update({
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "OAUTHBEARER",
            "oauth_cb": confluent_oauth_callback,
        })
    elif AUTH_TYPE == "PLAINTEXT":
        logger.info("Using PLAINTEXT security protocol.")

    return Consumer(config)


def run_consumer_loop():
    """Blocking consumer polling loop executed in a background thread."""
    global consumer_running
    consumer = None

    # Wait 2 seconds so FastAPI can bind to PORT and pass initial Cloud Run health checks
    time.sleep(2)

    while consumer_running:
        try:
            if consumer is None:
                logger.info(f"Connecting Consumer to {BOOTSTRAP_SERVERS} topic '{TOPIC_NAME}'...")
                consumer = create_kafka_consumer()
                consumer.subscribe([TOPIC_NAME])
                stats["is_consuming"] = True
                logger.info(f"Consumer subscribed to '{TOPIC_NAME}' with group '{GROUP_ID}'.")

            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                logger.error(f"Consumer message error: {msg.error()}")
                stats["last_error"] = str(msg.error())
                continue

            try:
                payload_str = msg.value().decode("utf-8")
                try:
                    payload_json = json.loads(payload_str)
                except Exception:
                    payload_json = payload_str

                partition = msg.partition()
                offset = msg.offset()
                key = msg.key().decode("utf-8") if msg.key() else None

                stats["messages_consumed"] += 1
                stats["offsets_by_partition"][partition] = offset
                stats["last_consumed_message"] = {
                    "topic": msg.topic(),
                    "partition": partition,
                    "offset": offset,
                    "key": key,
                    "payload": payload_json,
                }

                logger.info(
                    f"[CONSUMED] Topic: {msg.topic()} | Partition: {partition} | Offset: {offset} | "
                    f"Key: {key} | OrderID: {payload_json.get('order_id') if isinstance(payload_json, dict) else 'N/A'}"
                )
            except Exception as e:
                logger.error(f"Error parsing consumed message: {e}")
                stats["last_error"] = str(e)

        except Exception as exc:
            logger.error(f"Exception in consumer loop: {exc}. Retrying in 5 seconds...")
            stats["last_error"] = str(exc)
            stats["is_consuming"] = False
            if consumer:
                try:
                    consumer.close()
                except Exception:
                    pass
                consumer = None
            time.sleep(5)

    if consumer:
        logger.info("Closing Kafka consumer on thread shutdown...")
        try:
            consumer.close()
        except Exception:
            pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifecycle manager for FastAPI startup and consumer thread shutdown."""
    global consumer_running, consumer_thread
    logger.info(f"Starting Consumer Application on port {PORT}...")
    consumer_running = True
    consumer_thread = threading.Thread(target=run_consumer_loop, daemon=True)
    consumer_thread.start()

    yield

    consumer_running = False
    if consumer_thread and consumer_thread.is_alive():
        consumer_thread.join(timeout=3)


app = FastAPI(
    title="Kafka POC Consumer Service",
    description="Consumes order events from Google Cloud Managed Service for Apache Kafka.",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/")
@app.get("/health")
def health():
    return {
        "status": "UP",
        "service": "kafka-consumer",
        "config": {
            "bootstrap_servers": BOOTSTRAP_SERVERS,
            "topic": TOPIC_NAME,
            "group_id": GROUP_ID,
            "auth_type": AUTH_TYPE,
            "auto_offset_reset": AUTO_OFFSET_RESET,
        },
        "stats": stats,
    }


@app.get("/status")
def get_status():
    """Returns detailed statistics and latest consumed payload information."""
    return stats


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)

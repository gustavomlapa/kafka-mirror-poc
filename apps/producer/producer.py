"""Producer Application for Google Cloud Managed Service for Apache Kafka POC.

Provides continuous event streaming and REST endpoints to trigger mock orders.
"""

import asyncio
import json
import logging
import os
import sys
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional

from fastapi import BackgroundTasks, FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn

# Include parent directory in sys.path to access auth helper
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from auth.gcp_token_provider import confluent_oauth_callback
from producer.mock_data import MockDataGenerator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("kafka-producer-app")

# Configuration via Environment Variables
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "orders-poc")
AUTH_TYPE = os.getenv("KAFKA_AUTH_TYPE", "OAUTHBEARER").upper()
AUTO_STREAM = os.getenv("AUTO_STREAM", "true").lower() == "true"
STREAM_INTERVAL = float(os.getenv("STREAM_INTERVAL_SECONDS", "3.0"))
PORT = int(os.getenv("PORT", "8080"))

generator = MockDataGenerator()
producer_client = None
stream_task: Optional[asyncio.Task] = None
stats = {
    "messages_produced": 0,
    "last_produced_event": None,
    "last_error": None,
    "is_streaming": False,
    "bootstrap_servers": BOOTSTRAP_SERVERS,
    "topic": TOPIC_NAME,
    "auth_type": AUTH_TYPE,
}


def create_kafka_producer():
    """Initializes the confluent_kafka Producer instance with appropriate auth."""
    from confluent_kafka import Producer

    config = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "client.id": "cloudrun-kafka-producer",
        "acks": "all",
        "retries": 5,
        "retry.backoff.ms": 500,
    }

    if AUTH_TYPE == "OAUTHBEARER":
        logger.info("Configuring SASL_SSL OAUTHBEARER authentication with GCP ADC...")
        config.update({
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "OAUTHBEARER",
            "oauth_cb": confluent_oauth_callback,
        })
    elif AUTH_TYPE == "PLAINTEXT":
        logger.info("Using PLAINTEXT security protocol.")
    else:
        logger.warning(f"Unknown AUTH_TYPE '{AUTH_TYPE}'. Defaulting to PLAINTEXT.")

    return Producer(config)


def delivery_report(err, msg):
    """Callback invoked when message is acknowledged or fails."""
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
        stats["last_error"] = str(err)
    else:
        stats["messages_produced"] += 1
        logger.info(
            f"Message delivered to {msg.topic()} [partition {msg.partition()}] at offset {msg.offset()}"
        )


def send_event(event_data: Dict[str, Any]) -> Dict[str, Any]:
    """Serializes and sends an event to Kafka."""
    global producer_client
    if producer_client is None:
        producer_client = create_kafka_producer()

    payload_json = json.dumps(event_data).encode("utf-8")
    key = str(event_data.get("order_id", "")).encode("utf-8")

    try:
        producer_client.produce(
            topic=TOPIC_NAME,
            key=key,
            value=payload_json,
            on_delivery=delivery_report,
        )
        producer_client.poll(0)
        stats["last_produced_event"] = event_data
        return {"status": "queued", "event": event_data}
    except Exception as exc:
        logger.error(f"Failed to produce message: {exc}")
        stats["last_error"] = str(exc)
        raise exc


async def background_streaming_loop():
    """Background task to continuously produce mock events at fixed intervals."""
    stats["is_streaming"] = True
    logger.info(f"Starting continuous stream to topic '{TOPIC_NAME}' every {STREAM_INTERVAL}s...")
    try:
        while True:
            order = generator.generate_order()
            try:
                send_event(order)
            except Exception as e:
                logger.warning(f"Stream iteration error: {e}")
            await asyncio.sleep(STREAM_INTERVAL)
    except asyncio.CancelledError:
        logger.info("Streaming background loop stopped.")
    finally:
        stats["is_streaming"] = False


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifecycle manager for FastAPI application startup and shutdown."""
    global producer_client, stream_task
    try:
        producer_client = create_kafka_producer()
        logger.info(f"Connected producer targeting bootstrap servers: {BOOTSTRAP_SERVERS}")
    except Exception as exc:
        logger.error(f"Initialization warning: {exc}")

    if AUTO_STREAM:
        stream_task = asyncio.create_task(background_streaming_loop())

    yield

    if stream_task and not stream_task.done():
        stream_task.cancel()
        try:
            await stream_task
        except asyncio.CancelledError:
            pass

    if producer_client:
        logger.info("Flushing Kafka producer on shutdown...")
        producer_client.flush(timeout=5)


app = FastAPI(
    title="Kafka POC Producer Service",
    description="Produces mock order events to Google Cloud Managed Service for Apache Kafka.",
    version="1.0.0",
    lifespan=lifespan,
)


class ProduceRequest(BaseModel):
    count: int = 1
    custom_order_id: Optional[str] = None


@app.get("/")
@app.get("/health")
def health():
    return {
        "status": "UP",
        "service": "kafka-producer",
        "config": {
            "bootstrap_servers": BOOTSTRAP_SERVERS,
            "topic": TOPIC_NAME,
            "auth_type": AUTH_TYPE,
            "stream_interval_seconds": STREAM_INTERVAL,
        },
        "stats": stats,
    }


@app.post("/produce")
def produce_events(request: ProduceRequest):
    """Endpoint to trigger generation of one or more mock events on demand."""
    results = []
    for _ in range(request.count):
        order = generator.generate_order(custom_order_id=request.custom_order_id)
        res = send_event(order)
        results.append(res)

    if producer_client:
        producer_client.flush(timeout=3)

    return {
        "produced_count": len(results),
        "events": results,
    }


@app.post("/stream/start")
def start_stream():
    """Starts the continuous background streaming loop."""
    global stream_task
    if stream_task and not stream_task.done():
        return {"status": "already_running"}
    stream_task = asyncio.create_task(background_streaming_loop())
    return {"status": "started"}


@app.post("/stream/stop")
def stop_stream():
    """Stops the continuous background streaming loop."""
    global stream_task
    if stream_task and not stream_task.done():
        stream_task.cancel()
        return {"status": "stopped"}
    return {"status": "not_running"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)

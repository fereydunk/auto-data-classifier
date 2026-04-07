"""
Kafka pipeline: reads from Confluent Cloud, classifies via local service,
routes to sensitivity-tiered output topics, and tags schema fields
in the Confluent Stream Catalog.
"""

import asyncio
import json
import logging
import signal
import struct
from typing import Any, Dict, Optional

import httpx
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

from config import Config
from catalog_tagger import extract_schema_id_from_wire

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger("pipeline")

cfg = Config()



# ---------------------------------------------------------------------------
# Schema Registry — Avro deserializer
# ---------------------------------------------------------------------------
def build_avro_deserializer() -> AvroDeserializer:
    sr_client = SchemaRegistryClient({
        "url": cfg.SR_URL,
        "basic.auth.user.info": f"{cfg.SR_API_KEY}:{cfg.SR_API_SECRET}",
    })
    # schema_str=None → deserializer fetches schema from SR using the wire schema ID
    return AvroDeserializer(sr_client, schema_str=None, from_dict=lambda d, _: d)


def deserialize(
    raw: bytes,
    topic: str,
    avro_deserializer: AvroDeserializer,
) -> tuple[Optional[Dict[str, Any]], Optional[int]]:
    """
    Deserialize a Kafka message value.

    Returns (payload_dict, schema_id).
    - Confluent wire-format (Avro): deserialized via Schema Registry.
    - Plain JSON: parsed directly; schema_id will be None.
    """
    schema_id = extract_schema_id_from_wire(raw)

    if schema_id is not None:
        try:
            ctx = SerializationContext(topic, MessageField.VALUE)
            payload = avro_deserializer(raw, ctx)
            return payload, schema_id
        except Exception as e:
            logger.warning("Avro deserialization failed (offset unknown): %s — falling back to JSON", e)

    # Fallback: plain JSON
    try:
        return json.loads(raw.decode("utf-8")), None
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None, None


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------
async def classify_message(
    client: httpx.AsyncClient,
    payload: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    try:
        response = await client.post(
            f"{cfg.CLASSIFIER_URL}/classify",
            json={"fields": payload, "max_layer": cfg.MAX_LAYER},
            timeout=cfg.CLASSIFIER_TIMEOUT_S,
        )
        response.raise_for_status()
        return response.json()
    except httpx.HTTPError as e:
        logger.error("Classifier call failed: %s", e)
        return None


# ---------------------------------------------------------------------------
# Message routing
# ---------------------------------------------------------------------------
def route_message(
    producer: Producer,
    original_payload: Dict[str, Any],
    classification: Dict[str, Any],
    source_key: Optional[bytes],
) -> None:
    tags = classification.get("tags", [])
    sink_topic = cfg.SINK_TOPIC_CLASSIFIED if tags else cfg.SINK_TOPIC_SAFE

    enriched = {
        "payload": original_payload,
        "classification": {
            "tags": tags,
            "detected_entities": classification.get("detected_entities", {}),
            "classified_at": classification.get("classified_at"),
            "classifier_version": classification.get("classifier_version"),
        },
    }

    producer.produce(
        topic=sink_topic,
        key=source_key,
        value=json.dumps(enriched).encode("utf-8"),
        on_delivery=_delivery_report,
    )

    audit = {
        "tags": tags,
        "sink_topic": sink_topic,
        "classified_at": classification.get("classified_at"),
        "entity_types_found": list(
            {
                e["entity_type"]
                for entities in classification.get("detected_entities", {}).values()
                for e in entities
            }
        ),
    }
    producer.produce(
        topic=cfg.SINK_TOPIC_AUDIT,
        key=source_key,
        value=json.dumps(audit).encode("utf-8"),
        on_delivery=_delivery_report,
    )


def _delivery_report(err, msg):
    if err:
        logger.error("Delivery failed [%s]: %s", msg.topic(), err)
    else:
        logger.debug("Delivered to %s [%d] @ %d", msg.topic(), msg.partition(), msg.offset())


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
async def post_recommendations(
    client: httpx.AsyncClient,
    topic: str,
    detected_entities: Dict[str, Any],
    schema_id: Optional[int],
) -> None:
    """
    For each (field, tag) pair in detected_entities, POST one recommendation
    to the review API using the highest-confidence entity for that pair.
    The source layer that produced that best result is recorded.
    """
    subject = f"{topic}-value"
    for field_path, entities in detected_entities.items():
        # Group by tag — keep the highest-confidence entity per tag
        tag_best: Dict[str, Any] = {}
        for entity in entities:
            tag = entity.get("tag", "PII")
            if tag not in tag_best or entity["score"] > tag_best[tag]["score"]:
                tag_best[tag] = entity

        for tag, entity in tag_best.items():
            try:
                await client.post(
                    f"{cfg.REVIEW_API_URL}/recommendations",
                    json={
                        "topic": topic,
                        "subject": subject,
                        "schema_id": schema_id,
                        "field_path": field_path,
                        "proposed_tag": tag,
                        "entity_type": entity["entity_type"],
                        "confidence": entity["score"],
                        "layer": entity.get("layer", 3),
                        "source": entity.get("source", "ai_model"),
                        "is_free_text": entity.get("is_free_text", False),
                    },
                    timeout=5.0,
                )
            except httpx.HTTPError as e:
                logger.warning(
                    "Failed to post recommendation for %s → %s: %s", field_path, tag, e
                )


async def run():
    avro_deserializer = build_avro_deserializer()
    consumer = Consumer(cfg.kafka_consumer_config)
    producer = Producer(cfg.kafka_producer_config)
    consumer.subscribe([cfg.SOURCE_TOPIC])

    logger.info(
        "Subscribed to %s — classified→%s | safe→%s | audit→%s",
        cfg.SOURCE_TOPIC,
        cfg.SINK_TOPIC_CLASSIFIED,
        cfg.SINK_TOPIC_SAFE,
        cfg.SINK_TOPIC_AUDIT,
    )

    shutdown = asyncio.Event()

    def _sigterm(*_):
        logger.info("Shutdown signal received")
        shutdown.set()

    signal.signal(signal.SIGTERM, _sigterm)
    signal.signal(signal.SIGINT, _sigterm)

    semaphore = asyncio.Semaphore(cfg.MAX_CONCURRENT_CLASSIFICATIONS)

    async with httpx.AsyncClient() as http_client:
        pending: list[asyncio.Task] = []

        while not shutdown.is_set():
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                # Flush partial batch when idle — avoids stalling on < BATCH_SIZE messages
                if pending:
                    await asyncio.gather(*pending)
                    pending.clear()
                    producer.flush()
                    consumer.commit(asynchronous=False)
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            raw_value = msg.value()
            if not raw_value:
                consumer.commit(asynchronous=False)
                continue

            payload, schema_id = deserialize(raw_value, cfg.SOURCE_TOPIC, avro_deserializer)

            if payload is None:
                logger.warning("Unreadable message at offset %d — skipped", msg.offset())
                consumer.commit(asynchronous=False)
                continue

            async def process(p=payload, k=msg.key(), sid=schema_id):
                async with semaphore:
                    result = await classify_message(http_client, p)
                    if result:
                        route_message(producer, p, result, k)

                        # Post recommendations to the review API (non-blocking best-effort)
                        detected = result.get("detected_entities", {})
                        if detected:
                            await post_recommendations(
                                http_client, cfg.SOURCE_TOPIC, detected, sid
                            )
                    else:
                        logger.warning("Classification failed — message skipped")

            task = asyncio.create_task(process())
            pending.append(task)

            if len(pending) >= cfg.BATCH_SIZE:
                await asyncio.gather(*pending)
                pending.clear()
                producer.flush()
                consumer.commit(asynchronous=False)

        # Drain remaining
        if pending:
            await asyncio.gather(*pending)
        producer.flush()
        consumer.commit(asynchronous=False)
        consumer.close()
        logger.info("Pipeline shut down cleanly.")


if __name__ == "__main__":
    asyncio.run(run())

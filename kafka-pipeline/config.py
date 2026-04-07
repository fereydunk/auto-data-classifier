import os


def _require(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise RuntimeError(f"Required environment variable '{key}' is not set")
    return val


class Config:
    # Confluent Cloud Kafka
    BOOTSTRAP_SERVERS: str = _require("CONFLUENT_BOOTSTRAP_SERVERS")
    SASL_USERNAME: str = _require("CONFLUENT_API_KEY")
    SASL_PASSWORD: str = _require("CONFLUENT_API_SECRET")

    # Schema Registry
    SR_URL: str = _require("CONFLUENT_SR_URL")
    SR_API_KEY: str = _require("CONFLUENT_SR_API_KEY")
    SR_API_SECRET: str = _require("CONFLUENT_SR_API_SECRET")
    SR_CLUSTER_ID: str = _require("CONFLUENT_SR_CLUSTER_ID")  # e.g. lsrc-xxxxx

    # Topics
    SOURCE_TOPIC: str = os.getenv("SOURCE_TOPIC", "raw-messages")
    SINK_TOPIC_CLASSIFIED: str = os.getenv("SINK_TOPIC_CLASSIFIED", "classified-messages")
    SINK_TOPIC_SAFE: str = os.getenv("SINK_TOPIC_SAFE", "classified-safe")
    SINK_TOPIC_AUDIT: str = os.getenv("SINK_TOPIC_AUDIT", "classification-audit")

    # Consumer group
    CONSUMER_GROUP: str = os.getenv("CONSUMER_GROUP", "auto-data-classifier")

    # Classifier service (local)
    CLASSIFIER_URL: str = os.getenv("CLASSIFIER_URL", "http://localhost:8000")

    # Review API (local)
    REVIEW_API_URL: str = os.getenv("REVIEW_API_URL", "http://localhost:8001")

    # Classification depth: 1 = field name only, 2 = +regex, 3 = +AI (default)
    MAX_LAYER: int = int(os.getenv("MAX_LAYER", "3"))

    # Tuning
    BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", "50"))
    CLASSIFIER_TIMEOUT_S: float = float(os.getenv("CLASSIFIER_TIMEOUT_S", "5.0"))
    MAX_CONCURRENT_CLASSIFICATIONS: int = int(os.getenv("MAX_CONCURRENT", "10"))


    @property
    def kafka_consumer_config(self) -> dict:
        return {
            "bootstrap.servers": self.BOOTSTRAP_SERVERS,
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": self.SASL_USERNAME,
            "sasl.password": self.SASL_PASSWORD,
            "group.id": self.CONSUMER_GROUP,
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        }

    @property
    def kafka_producer_config(self) -> dict:
        return {
            "bootstrap.servers": self.BOOTSTRAP_SERVERS,
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": self.SASL_USERNAME,
            "sasl.password": self.SASL_PASSWORD,
            "acks": "all",
            "retries": 5,
            "retry.backoff.ms": 300,
        }

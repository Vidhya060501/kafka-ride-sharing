# rider-service/kafka_producer.py

from confluent_kafka import Producer
import json
import time
from typing import Callable


def _delivery_report(err, msg):
    if err is not None:
        # In a real system, we’d log this with proper logging
        print(f"❌ Delivery failed for record {msg.key()}: {err}")
    else:
        print(
            f"✅ Message delivered to {msg.topic()} [{msg.partition()}] "
            f"offset {msg.offset()}"
        )


class RideRequestProducer:
    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self.topic = "ride-requests"

        # Idempotent producer configuration
        self._producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "enable.idempotence": True,       # idempotent producer
            "acks": "all",                    # strongest durability
            "max.in.flight.requests.per.connection": 5,
            "retries": 10,
            "linger.ms": 5,
            "batch.num.messages": 1000,
        })

    def _produce_with_retry(
        self,
        key: str,
        value: dict,
        max_attempts: int = 5,
        backoff_base: float = 0.2
    ):
        """
        Simple exponential backoff retry in case of transient failures
        (e.g., buffer full, broker temporarily unavailable).
        """
        payload = json.dumps(value).encode("utf-8")

        attempt = 0
        while attempt < max_attempts:
            try:
                self._producer.produce(
                    topic=self.topic,
                    key=key.encode("utf-8"),
                    value=payload,
                    on_delivery=_delivery_report,
                )
                # Trigger delivery callbacks
                self._producer.poll(0)
                return
            except BufferError:
                attempt += 1
                sleep_time = backoff_base * (2 ** (attempt - 1))
                print(
                    f"⚠️ Buffer full. Retrying in {sleep_time:.2f}s "
                    f"(attempt {attempt}/{max_attempts})"
                )
                time.sleep(sleep_time)

        raise RuntimeError("Failed to send message after retries")

    def send_ride_request(self, ride_request: dict):
        rider_id = ride_request["rider_id"]
        self._produce_with_retry(key=rider_id, value=ride_request)

    def flush(self):
        self._producer.flush()
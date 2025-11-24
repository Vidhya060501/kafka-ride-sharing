# trip-management/trip_management.py

from confluent_kafka import Consumer, Producer
from pydantic import BaseModel
from typing import Dict
import json
import time
import uuid


# ---------- Models ----------

class RideMatch(BaseModel):
    match_id: str
    rider_id: str
    driver_id: str
    pickup_lat: float
    pickup_lon: float
    driver_lat: float
    driver_lon: float
    estimated_eta_seconds: int
    timestamp: int
    surge_multiplier: float  # ✅ NEW: include surge from matching engine


class TripEvent(BaseModel):
    trip_id: str
    rider_id: str
    driver_id: str
    status: str          # ACCEPTED, STARTED, COMPLETED, CANCELLED
    timestamp: int       # event time (epoch millis)
    match_id: str        # link back to match event
    surge_multiplier: float  # ✅ NEW: carry surge into trips


# ---------- Kafka setup ----------

def create_consumer() -> Consumer:
    config = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "trip-management",
        "auto.offset.reset": "earliest",
    }
    return Consumer(config)


def create_producer() -> Producer:
    config = {
        "bootstrap.servers": "localhost:9092",
        "enable.idempotence": True,
        "acks": "all",
        "max.in.flight.requests.per.connection": 5,
        "retries": 10,
        "linger.ms": 5,
        "batch.num.messages": 1000,
    }
    return Producer(config)


def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Trip event delivery failed for {msg.key()}: {err}")
    else:
        print(
            f"✅ Trip event delivered to {msg.topic()} "
            f"[{msg.partition()}] offset {msg.offset()} key={msg.key().decode()}"
        )


# ---------- Main logic ----------

def emit_trip_lifecycle(
    producer: Producer,
    match: RideMatch,
    topic: str = "trip-events",
):
    """
    For each ride match, simulate a trip lifecycle:
      ACCEPTED -> STARTED -> COMPLETED
    with 1 second between each state.
    """
    trip_id = str(uuid.uuid4())
    driver_key = match.driver_id.encode("utf-8")

    statuses = ["ACCEPTED", "STARTED", "COMPLETED"]

    for status in statuses:
        event = TripEvent(
            trip_id=trip_id,
            rider_id=match.rider_id,
            driver_id=match.driver_id,
            status=status,
            timestamp=int(time.time() * 1000),
            match_id=match.match_id,
            surge_multiplier=match.surge_multiplier,  # ✅ propagate surge
        )

        producer.produce(
            topic=topic,
            key=driver_key,  # keep all trip events for a driver in same partition
            value=event.model_dump_json().encode("utf-8"),
            on_delivery=delivery_report,
        )
        producer.poll(0)

        print(f"📤 Emitted trip event: {event}")
        time.sleep(1)  # simulate time gaps between statuses


def main():
    consumer = create_consumer()
    producer = create_producer()

    topic = "ride-matches"
    consumer.subscribe([topic])
    print(f"🧭 Trip Management subscribed to: {topic}")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            if msg.error():
                print(f"⚠️ Consumer error: {msg.error()}")
                continue

            key = msg.key().decode("utf-8") if msg.key() else None
            value_str = msg.value().decode("utf-8")

            print(f"\n📥 Received match for trip management (key={key}): {value_str}")

            try:
                payload = json.loads(value_str)
                match = RideMatch(**payload)
            except Exception as e:
                print(f"❌ Failed to parse ride match: {e}")
                continue

            emit_trip_lifecycle(producer, match)

    except KeyboardInterrupt:
        print("\n🛑 Stopping Trip Management...")
    finally:
        print("Flushing producer...")
        producer.flush()
        consumer.close()
        print("Trip Management stopped.")


if __name__ == "__main__":
    main()
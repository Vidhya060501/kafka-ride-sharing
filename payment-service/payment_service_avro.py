# payment-service/payment_service_avro.py

from confluent_kafka import Consumer
from confluent_kafka.avro import AvroProducer
from pydantic import BaseModel
import json
import time
import uuid
import random
from typing import Tuple

from schemas import payment_value_schema  # from schemas.py


# ---------- Models (internal only) ----------

class TripEvent(BaseModel):
    trip_id: str
    rider_id: str
    driver_id: str
    status: str        # ACCEPTED, STARTED, COMPLETED, CANCELLED
    timestamp: int
    match_id: str
    surge_multiplier: float


# ---------- Kafka setup ----------

def create_consumer() -> Consumer:
    config = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "payment-service-avro",
        "auto.offset.reset": "earliest",
    }
    return Consumer(config)


def create_avro_producer() -> AvroProducer:
    config = {
        "bootstrap.servers": "localhost:9092",
        "schema.registry.url": "http://localhost:8081",
        # no idempotence here to avoid PID noise; your JSON payment service already
        # demonstrates transactional/idempotent behavior for the business context
        "linger.ms": 5,
        "batch.num.messages": 1000,
    }
    return AvroProducer(config, default_value_schema=payment_value_schema)


def delivery_report(err, msg):
    if err is not None:
        print(f"❌ [Avro] Payment delivery failed for {msg.key()}: {err}")
    else:
        print(
            f"✅ [Avro] Payment delivered to {msg.topic()} "
            f"[{msg.partition()}] offset {msg.offset()} key={msg.key().decode()}"
        )


# ---------- Payment logic ----------

def simulate_payment_gateway(trip: TripEvent) -> Tuple[float, float]:
    """
    Fake payment gateway:
      - Returns (base_fare, final_amount)
      - If rider_id starts with 'fail-', simulate a failure.
    """
    if trip.rider_id.startswith("fail-"):
        raise RuntimeError("Simulated payment gateway failure (Avro)")

    base_fare = round(random.uniform(10.0, 40.0), 2)
    surge = max(trip.surge_multiplier, 1.0)
    final_amount = round(base_fare * surge, 2)

    print(
        f"💸 [Avro] Calculated fare for trip_id={trip.trip_id}: "
        f"base_fare={base_fare}, surge={surge}, amount={final_amount}"
    )

    return base_fare, final_amount


def main():
    consumer = create_consumer()
    producer = create_avro_producer()

    topic_in = "trip-events"
    topic_out = "payments-avro"

    consumer.subscribe([topic_in])
    print(f"💳 [Avro] Payment Service subscribed to: {topic_in}")
    print(f"💳 [Avro] Producing payments to: {topic_out}")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            if msg.error():
                print(f"⚠️ Consumer error: {msg.error()}")
                continue

            value_str = msg.value().decode("utf-8")
            try:
                payload = json.loads(value_str)
                trip = TripEvent(**payload)
            except Exception as e:
                print(f"❌ Failed to parse trip event (Avro service): {e}")
                continue

            if trip.status != "COMPLETED":
                print(f"ℹ️ [Avro] Ignoring trip status={trip.status} (no payment yet)")
                continue

            try:
                base_fare, amount = simulate_payment_gateway(trip)
            except Exception as e:
                print(f"❌ [Avro] Payment simulation failed: {e}")
                # In a real system we'd send to DLQ; skip for now
                continue

            now = int(time.time() * 1000)
            payment_id = str(uuid.uuid4())

            # Dict must match the Avro schema exactly
            value = {
                "payment_id": payment_id,
                "trip_id": trip.trip_id,
                "rider_id": trip.rider_id,
                "driver_id": trip.driver_id,
                "amount": amount,
                "currency": "USD",
                "status": "SUCCESS",
                "timestamp": now,
                "base_fare": base_fare,
                "surge_multiplier": float(trip.surge_multiplier),
            }

            key = trip.trip_id.encode("utf-8")

            producer.produce(
                topic=topic_out,
                key=key,
                value=value,
                callback=delivery_report,
            )
            # Flush a little without blocking too much
            producer.flush(0)

    except KeyboardInterrupt:
        print("\n🛑 Stopping Avro Payment Service...")
    finally:
        print("Flushing Avro producer...")
        producer.flush()
        consumer.close()
        print("Avro Payment Service stopped.")


if __name__ == "__main__":
    main()
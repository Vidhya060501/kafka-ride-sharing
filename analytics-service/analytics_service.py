# analytics-service/analytics_service.py

from confluent_kafka import Consumer, Producer
from pydantic import BaseModel
import json
import time
from typing import Dict


# ---------- Models ----------

class PaymentEvent(BaseModel):
    payment_id: str
    trip_id: str
    rider_id: str
    driver_id: str
    amount: float
    currency: str
    status: str
    timestamp: int
    base_fare: float
    surge_multiplier: float


class AnalyticsEvent(BaseModel):
    event_id: str
    total_revenue: float
    total_trips_paid: int
    revenue_per_surge_bucket: Dict[str, float]
    trips_per_surge_bucket: Dict[str, int]
    timestamp: int


# ---------- Kafka setup ----------

def create_consumer() -> Consumer:
    config = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "analytics-service",
        "auto.offset.reset": "earliest",
    }
    return Consumer(config)


def create_producer() -> Producer:
    config = {
        "bootstrap.servers": "localhost:9092",
    }
    return Producer(config)


def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Analytics delivery failed for {msg.key()}: {err}")
    else:
        print(
            f"✅ Analytics event delivered to {msg.topic()} "
            f"[{msg.partition()}] offset {msg.offset()}"
        )


# ---------- Surge bucket helpers ----------

def surge_bucket(surge: float) -> str:
    """
    Bucket surge multipliers into human-readable ranges.
    Examples:
      1.0              -> "1.0x"
      1.01 .. 1.49     -> "1.0–1.5x"
      1.5  .. 1.99     -> "1.5–2.0x"
      2.0  .. 2.99     -> "2.0–3.0x"
      >= 3.0           -> "3.0x+"
    """
    if surge <= 1.0 + 1e-6:
        return "1.0x"
    elif surge < 1.5:
        return "1.0–1.5x"
    elif surge < 2.0:
        return "1.5–2.0x"
    elif surge < 3.0:
        return "2.0–3.0x"
    else:
        return "3.0x+"


# ---------- Analytics logic ----------

def main():
    consumer = create_consumer()
    producer = create_producer()

    topic = "payments"
    consumer.subscribe([topic])
    print(f"📊 Analytics Service subscribed to: {topic}")

    # Global aggregates
    total_revenue = 0.0
    total_trips_paid = 0

    # Surge-based aggregates
    revenue_per_surge_bucket: Dict[str, float] = {}
    trips_per_surge_bucket: Dict[str, int] = {}

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
                payment = PaymentEvent(**payload)
            except Exception as e:
                print(f"❌ Failed to parse payment event: {e}")
                continue

            print(f"\n📥 Received payment for analytics: {payment}")

            if payment.status != "SUCCESS":
                print(f"ℹ️ Ignoring payment with status={payment.status}")
                continue

            # ----- Update aggregates -----
            total_revenue += payment.amount
            total_trips_paid += 1

            bucket = surge_bucket(payment.surge_multiplier)
            revenue_per_surge_bucket[bucket] = (
                revenue_per_surge_bucket.get(bucket, 0.0) + payment.amount
            )
            trips_per_surge_bucket[bucket] = (
                trips_per_surge_bucket.get(bucket, 0) + 1
            )

            # ----- Emit analytics event -----
            now = int(time.time() * 1000)
            event_id = f"analytics-{now}"

            analytics = AnalyticsEvent(
                event_id=event_id,
                total_revenue=round(total_revenue, 2),
                total_trips_paid=total_trips_paid,
                revenue_per_surge_bucket={
                    k: round(v, 2) for k, v in revenue_per_surge_bucket.items()
                },
                trips_per_surge_bucket=trips_per_surge_bucket.copy(),
                timestamp=now,
            )

            out_topic = "analytics-events"
            producer.produce(
                topic=out_topic,
                key=b"global-metrics",
                value=analytics.model_dump_json().encode("utf-8"),
                on_delivery=delivery_report,
            )
            producer.poll(0)

            print(f"📤 Emitted analytics event: {analytics}")
            print(f"   Revenue per surge bucket: {analytics.revenue_per_surge_bucket}")
            print(f"   Trips per surge bucket  : {analytics.trips_per_surge_bucket}")

    except KeyboardInterrupt:
        print("\n🛑 Stopping Analytics Service...")
    finally:
        print("Flushing producer...")
        producer.flush()
        consumer.close()
        print("Analytics Service stopped.")


if __name__ == "__main__":
    main()
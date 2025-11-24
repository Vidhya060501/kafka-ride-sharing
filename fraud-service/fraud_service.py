# fraud-service/fraud_service.py

from confluent_kafka import Consumer, Producer
from pydantic import BaseModel
import json
import time
from typing import Dict, List


# ---------- Models ----------

class TripEvent(BaseModel):
    trip_id: str
    rider_id: str
    driver_id: str
    status: str          # ACCEPTED, STARTED, COMPLETED, CANCELLED
    timestamp: int
    match_id: str
    surge_multiplier: float


class PaymentEvent(BaseModel):
    payment_id: str
    trip_id: str
    rider_id: str
    driver_id: str
    amount: float
    currency: str
    status: str          # SUCCESS, FAILED, ...
    timestamp: int
    base_fare: float
    surge_multiplier: float


class FraudAlert(BaseModel):
    alert_id: str
    type: str
    severity: str
    rider_id: str | None
    driver_id: str | None
    trip_id: str | None
    description: str
    timestamp: int
    metadata: dict = {}


# ---------- Kafka setup ----------

def create_consumer() -> Consumer:
    config = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "fraud-service",
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
        print(f"❌ Fraud alert delivery failed for {msg.key()}: {err}")
    else:
        print(
            f"✅ Fraud alert delivered to {msg.topic()} "
            f"[{msg.partition()}] offset {msg.offset()}"
        )


# ---------- Simple fraud rules ----------

def cleanup_old_events(events: List[int], window_ms: int, now_ms: int) -> List[int]:
    """Keep only events within [now_ms - window_ms, now_ms]."""
    cutoff = now_ms - window_ms
    return [t for t in events if t >= cutoff]


def main():
    consumer = create_consumer()
    producer = create_producer()

    topics = ["trip-events", "payments"]
    consumer.subscribe(topics)
    print(f"🕵️ Fraud Detection Service subscribed to: {topics}")

    # State: cancellation windows per rider/driver
    rider_cancels: Dict[str, List[int]] = {}
    driver_cancels: Dict[str, List[int]] = {}

    CANCEL_WINDOW_MS = 10 * 60 * 1000  # 10 minutes
    CANCEL_THRESHOLD = 3               # >= 3 cancels in window -> alert

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            if msg.error():
                print(f"⚠️ Consumer error: {msg.error()}")
                continue

            topic = msg.topic()
            value_str = msg.value().decode("utf-8")

            try:
                payload = json.loads(value_str)
            except json.JSONDecodeError:
                print(f"❌ Failed to decode JSON from topic={topic}: {value_str}")
                continue

            now_ms = int(time.time() * 1000)

            # ---------- Trip-based rules ----------
            if topic == "trip-events":
                try:
                    trip = TripEvent(**payload)
                except Exception as e:
                    print(f"❌ Failed to parse trip event in fraud-service: {e}")
                    continue

                print(f"\n📥 Trip event in fraud-service: {trip}")

                if trip.status == "CANCELLED":
                    # Track rider cancels
                    rc = rider_cancels.get(trip.rider_id, [])
                    rc.append(trip.timestamp)
                    rc = cleanup_old_events(rc, CANCEL_WINDOW_MS, now_ms)
                    rider_cancels[trip.rider_id] = rc

                    # Track driver cancels (if we ever treat certain statuses as driver cancels)
                    dc = driver_cancels.get(trip.driver_id, [])
                    dc.append(trip.timestamp)
                    dc = cleanup_old_events(dc, CANCEL_WINDOW_MS, now_ms)
                    driver_cancels[trip.driver_id] = dc

                    if len(rc) >= CANCEL_THRESHOLD:
                        alert = FraudAlert(
                            alert_id=f"cancel-rider-{trip.rider_id}-{now_ms}",
                            type="EXCESSIVE_CANCELLATIONS_RIDER",
                            severity="MEDIUM",
                            rider_id=trip.rider_id,
                            driver_id=None,
                            trip_id=trip.trip_id,
                            description=(
                                f"Rider {trip.rider_id} has {len(rc)} cancellations "
                                f"in the last 10 minutes."
                            ),
                            timestamp=now_ms,
                            metadata={"cancel_timestamps": rc},
                        )
                        producer.produce(
                            topic="fraud-alerts",
                            key=trip.rider_id.encode("utf-8"),
                            value=alert.model_dump_json().encode("utf-8"),
                            on_delivery=delivery_report,
                        )
                        producer.poll(0)
                        print(f"🚨 Fraud alert (rider cancellations): {alert}")

            # ---------- Payment-based rules ----------
            elif topic == "payments":
                try:
                    payment = PaymentEvent(**payload)
                except Exception as e:
                    print(f"❌ Failed to parse payment event in fraud-service: {e}")
                    continue

                print(f"\n📥 Payment event in fraud-service: {payment}")

                if payment.status == "SUCCESS":
                    # Example rule: unusually high surge and fare
                    if payment.surge_multiplier >= 2.5 and payment.amount >= 60.0:
                        alert = FraudAlert(
                            alert_id=f"high-surge-{payment.trip_id}-{now_ms}",
                            type="HIGH_SURGE_HIGH_FARE",
                            severity="LOW",
                            rider_id=payment.rider_id,
                            driver_id=payment.driver_id,
                            trip_id=payment.trip_id,
                            description=(
                                f"Trip {payment.trip_id} has high surge "
                                f"(x{payment.surge_multiplier}) and amount "
                                f"{payment.amount:.2f} {payment.currency}."
                            ),
                            timestamp=now_ms,
                            metadata={
                                "amount": payment.amount,
                                "currency": payment.currency,
                                "base_fare": payment.base_fare,
                                "surge_multiplier": payment.surge_multiplier,
                            },
                        )
                        producer.produce(
                            topic="fraud-alerts",
                            key=payment.rider_id.encode("utf-8"),
                            value=alert.model_dump_json().encode("utf-8"),
                            on_delivery=delivery_report,
                        )
                        producer.poll(0)
                        print(f"🚨 Fraud alert (high surge / fare): {alert}")

                # You could also add rules on FAILED payments (e.g., many failures per rider)

    except KeyboardInterrupt:
        print("\n🛑 Stopping Fraud Detection Service...")
    finally:
        print("Flushing producer...")
        producer.flush()
        consumer.close()
        print("Fraud Detection Service stopped.")


if __name__ == "__main__":
    main()
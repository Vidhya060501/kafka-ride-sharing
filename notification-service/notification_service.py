# notification-service/notification_service.py

from confluent_kafka import Consumer, Producer
from pydantic import BaseModel
import json
import time
from typing import Literal


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
    surge_multiplier: float


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


class Notification(BaseModel):
    user_id: str
    channel: Literal["PUSH", "EMAIL", "SMS"]
    target: Literal["RIDER", "DRIVER"]
    type: str
    message: str
    timestamp: int
    metadata: dict = {}


# ---------- Kafka setup ----------

def create_consumer() -> Consumer:
    config = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "notification-service",
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
        print(f"❌ Notification delivery failed for {msg.key()}: {err}")
    else:
        print(
            f"✅ Notification delivered to {msg.topic()} "
            f"[{msg.partition()}] offset {msg.offset()}"
        )


# ---------- Notification builders ----------

def build_match_notification(match: RideMatch) -> Notification:
    msg = (
        f"Your driver {match.driver_id} has been assigned. "
        f"ETA ~{match.estimated_eta_seconds} seconds. "
        f"Current surge: x{match.surge_multiplier}."
    )
    return Notification(
        user_id=match.rider_id,
        channel="PUSH",
        target="RIDER",
        type="RIDE_MATCHED",
        message=msg,
        timestamp=int(time.time() * 1000),
        metadata={
            "match_id": match.match_id,
            "driver_id": match.driver_id,
        },
    )


def build_trip_notification(trip: TripEvent) -> Notification:
    if trip.status == "ACCEPTED":
        msg_type = "TRIP_ACCEPTED"
        msg_text = "Your driver has accepted your ride."
    elif trip.status == "STARTED":
        msg_type = "TRIP_STARTED"
        msg_text = "Your trip has started."
    elif trip.status == "COMPLETED":
        msg_type = "TRIP_COMPLETED"
        msg_text = "Your trip is completed. Thank you for riding with us!"
    elif trip.status == "CANCELLED":
        msg_type = "TRIP_CANCELLED"
        msg_text = "Your trip was cancelled."
    else:
        msg_type = f"TRIP_{trip.status}"
        msg_text = f"Trip status changed to {trip.status}."

    return Notification(
        user_id=trip.rider_id,
        channel="PUSH",
        target="RIDER",
        type=msg_type,
        message=msg_text,
        timestamp=int(time.time() * 1000),
        metadata={
            "trip_id": trip.trip_id,
            "match_id": trip.match_id,
            "surge_multiplier": trip.surge_multiplier,
        },
    )


def build_payment_notification(payment: PaymentEvent) -> Notification:
    if payment.status == "SUCCESS":
        msg_type = "PAYMENT_SUCCESS"
        msg_text = (
            f"Payment of {payment.amount:.2f} {payment.currency} succeeded "
            f"(surge x{payment.surge_multiplier})."
        )
    else:
        msg_type = "PAYMENT_FAILED"
        msg_text = (
            f"Payment of {payment.amount:.2f} {payment.currency} failed. "
            "Please update your payment method."
        )

    return Notification(
        user_id=payment.rider_id,
        channel="PUSH",
        target="RIDER",
        type=msg_type,
        message=msg_text,
        timestamp=int(time.time() * 1000),
        metadata={
            "trip_id": payment.trip_id,
            "payment_id": payment.payment_id,
            "base_fare": payment.base_fare,
            "surge_multiplier": payment.surge_multiplier,
        },
    )


# ---------- Main loop ----------

def main():
    consumer = create_consumer()
    producer = create_producer()

    topics = ["ride-matches", "trip-events", "payments"]
    consumer.subscribe(topics)
    print(f"🔔 Notification Service subscribed to: {topics}")

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

            notification: Notification | None = None

            try:
                if topic == "ride-matches":
                    match = RideMatch(**payload)
                    print(f"\n📥 Match event for notifications: {match}")
                    notification = build_match_notification(match)

                elif topic == "trip-events":
                    trip = TripEvent(**payload)
                    print(f"\n📥 Trip event for notifications: {trip}")
                    notification = build_trip_notification(trip)

                elif topic == "payments":
                    payment = PaymentEvent(**payload)
                    print(f"\n📥 Payment event for notifications: {payment}")
                    notification = build_payment_notification(payment)

            except Exception as e:
                print(f"❌ Failed to build notification from {topic}: {e}")
                continue

            if notification is None:
                continue

            out_topic = "notifications"
            producer.produce(
                topic=out_topic,
                key=notification.user_id.encode("utf-8"),
                value=notification.model_dump_json().encode("utf-8"),
                on_delivery=delivery_report,
            )
            producer.poll(0)

            print(f"📤 Emitted notification: {notification}")

    except KeyboardInterrupt:
        print("\n🛑 Stopping Notification Service...")
    finally:
        print("Flushing producer...")
        producer.flush()
        consumer.close()
        print("Notification Service stopped.")


if __name__ == "__main__":
    main()
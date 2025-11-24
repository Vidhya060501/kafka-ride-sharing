# payment-service/payment_service.py

from confluent_kafka import Consumer, Producer
from pydantic import BaseModel
import json
import time
import uuid
import random
from typing import Tuple


# ---------- Models ----------

class TripEvent(BaseModel):
    trip_id: str
    rider_id: str
    driver_id: str
    status: str        # ACCEPTED, STARTED, COMPLETED, CANCELLED
    timestamp: int
    match_id: str
    surge_multiplier: float  # from trip-events


class PaymentEvent(BaseModel):
    payment_id: str
    trip_id: str
    rider_id: str
    driver_id: str
    amount: float
    currency: str
    status: str        # PENDING, SUCCESS, FAILED
    timestamp: int
    base_fare: float           # before surge
    surge_multiplier: float    # surge used for this payment


class PaymentDlqEvent(BaseModel):
    """
    Event sent to payments-dlq when payment processing fails
    even after retries / transaction abort.
    """
    trip: TripEvent
    error: str
    timestamp: int


# ---------- Kafka setup ----------

def create_consumer() -> Consumer:
    config = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "payment-service",
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
        # transactional.id is required for transactions
        "transactional.id": "payment-service-1",
    }
    producer = Producer(config)
    # Initialize transactions once at startup
    print("💳 Initializing payment producer transactions...")
    producer.init_transactions()
    print("✅ Payment producer transactions initialized.")
    return producer


def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Payment delivery failed for {msg.key()}: {err}")
    else:
        print(
            f"✅ Payment delivered to {msg.topic()} "
            f"[{msg.partition()}] offset {msg.offset()} key={msg.key().decode()}"
        )


# ---------- Payment logic ----------

def simulate_payment_gateway(trip: TripEvent) -> Tuple[float, float]:
    """
    Fake payment gateway:
      - Returns (base_fare, final_amount)
      - Occasionally fails to demonstrate DLQ

    Rule: If rider_id starts with 'fail-', simulate a failure.
    """
    if trip.rider_id.startswith("fail-"):
        raise RuntimeError("Simulated payment gateway failure for testing DLQ")

    # Base fare between 10 and 40
    base_fare = round(random.uniform(10.0, 40.0), 2)

    surge = max(trip.surge_multiplier, 1.0)  # ensure at least 1.0
    final_amount = round(base_fare * surge, 2)

    print(
        f"💸 Calculated fare for trip_id={trip.trip_id}: "
        f"base_fare={base_fare}, surge={surge}, amount={final_amount}"
    )

    return base_fare, final_amount


def send_to_dlq(trip: TripEvent, error: str):
    """
    Non-transactional producer to send DLQ events.
    DLQ doesn't need exactly-once semantics in this demo.
    """
    dlq_config = {
        "bootstrap.servers": "localhost:9092",
    }
    dlq_producer = Producer(dlq_config)

    event = PaymentDlqEvent(
        trip=trip,
        error=error,
        timestamp=int(time.time() * 1000),
    )
    topic = "payments-dlq"
    key = trip.trip_id.encode("utf-8")

    dlq_producer.produce(
        topic=topic,
        key=key,
        value=event.model_dump_json().encode("utf-8"),
    )
    dlq_producer.flush()

    print(f"📥 Sent to DLQ ({topic}): {event}")


def process_completed_trip_tx(producer: Producer, trip: TripEvent):
    """
    Process payment for COMPLETED trip using a transactional producer.
    If anything fails, abort transaction and send trip to DLQ.
    """
    try:
        producer.begin_transaction()
        print(f"💸 Starting transaction for trip_id={trip.trip_id}")

        base_fare, amount = simulate_payment_gateway(trip)
        now = int(time.time() * 1000)
        payment_id = str(uuid.uuid4())

        payment = PaymentEvent(
            payment_id=payment_id,
            trip_id=trip.trip_id,
            rider_id=trip.rider_id,
            driver_id=trip.driver_id,
            amount=amount,
            currency="USD",
            status="SUCCESS",
            timestamp=now,
            base_fare=base_fare,
            surge_multiplier=trip.surge_multiplier,
        )

        topic = "payments"
        key = trip.trip_id.encode("utf-8")

        producer.produce(
            topic=topic,
            key=key,
            value=payment.model_dump_json().encode("utf-8"),
            on_delivery=delivery_report,
        )

        # In a real app, you'd also send offsets as part of the transaction.
        producer.commit_transaction()
        print(f"✅ Committed transaction for trip_id={trip.trip_id}")
        print(f"📤 Emitted payment event: {payment}")

    except Exception as e:
        print(f"❌ Error during payment transaction for trip_id={trip.trip_id}: {e}")
        try:
            producer.abort_transaction()
            print(f"⚠️ Aborted transaction for trip_id={trip.trip_id}")
        except Exception as abort_err:
            print(f"❌ Failed to abort transaction: {abort_err}")

        # Send the failed trip to DLQ for offline investigation
        send_to_dlq(trip, str(e))


def main():
    consumer = create_consumer()
    producer = create_producer()

    topic = "trip-events"
    consumer.subscribe([topic])
    print(f"💳 Payment Service subscribed to: {topic}")

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

            print(f"\n📥 Received trip event (key={key}): {value_str}")

            try:
                payload = json.loads(value_str)
                trip = TripEvent(**payload)
            except Exception as e:
                print(f"❌ Failed to parse trip event: {e}")
                continue

            if trip.status == "COMPLETED":
                process_completed_trip_tx(producer, trip)
            else:
                print(f"ℹ️ Ignoring trip status={trip.status} (no payment yet)")

    except KeyboardInterrupt:
        print("\n🛑 Stopping Payment Service...")
    finally:
        print("Flushing producer...")
        producer.flush()
        consumer.close()
        print("Payment Service stopped.")


if __name__ == "__main__":
    main()
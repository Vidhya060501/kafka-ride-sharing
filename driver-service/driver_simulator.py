# driver-service/driver_simulator.py

from confluent_kafka import Producer
import json
import time
import random


def delivery_report(err, msg):
    """Callback for Kafka to tell us if the message was delivered."""
    if err is not None:
        print(f"❌ Delivery failed for {msg.key()}: {err}")
    else:
        print(
            f"✅ Delivered to {msg.topic()} [{msg.partition()}] offset {msg.offset()} "
            f"key={msg.key().decode()}"
        )


def create_producer():
    """
    Create an idempotent Kafka producer configured for our cluster.
    """
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


def main():
    producer = create_producer()
    topic = "driver-locations"

    # We'll simulate 3 drivers around NYC
    drivers = [
        {"driver_id": "driver-1", "lat": 40.7128, "lon": -74.0060},   # Manhattan
        {"driver_id": "driver-2", "lat": 40.7306, "lon": -73.9352},   # Brooklyn-ish
        {"driver_id": "driver-3", "lat": 40.7580, "lon": -73.9855},   # Times Sq
    ]

    print("🚗 Starting driver location simulation. Press Ctrl+C to stop.")
    try:
        while True:
            for d in drivers:
                # Small random jitter to simulate movement
                d["lat"] += random.uniform(-0.0005, 0.0005)
                d["lon"] += random.uniform(-0.0005, 0.0005)

                payload = {
                    "driver_id": d["driver_id"],
                    "lat": d["lat"],
                    "lon": d["lon"],
                    "availability": "AVAILABLE",
                    "timestamp": int(time.time() * 1000),
                }

                key = d["driver_id"]  # 👈 driver_id as partition key

                producer.produce(
                    topic=topic,
                    key=key.encode("utf-8"),
                    value=json.dumps(payload).encode("utf-8"),
                    on_delivery=delivery_report,
                )
                # Let Kafka send stuff out
                producer.poll(0)

                print(f"📤 Sent location for {d['driver_id']}: {payload}")

            # Wait 5 seconds before next batch of updates
            time.sleep(5)

    except KeyboardInterrupt:
        print("\n🛑 Stopping driver simulation...")
    finally:
        print("Flushing pending messages...")
        producer.flush()
        print("Done.")


if __name__ == "__main__":
    main()
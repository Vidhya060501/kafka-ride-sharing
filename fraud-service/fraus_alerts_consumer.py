from confluent_kafka import Consumer
import json


def create_consumer():
    config = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "fraud-alerts-logger",
        "auto.offset.reset": "earliest",
    }
    return Consumer(config)


def main():
    consumer = create_consumer()
    topic = "fraud-alerts"
    consumer.subscribe([topic])
    print(f"👂 Subscribed to fraud alerts topic: {topic}")

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
            try:
                value = json.loads(value_str)
            except json.JSONDecodeError:
                value = value_str

            print("\n🚨 Fraud alert:")
            print(f"  key   : {key}")
            print(f"  value : {value}")

    except KeyboardInterrupt:
        print("\n🛑 Stopping fraud alerts consumer...")
    finally:
        consumer.close()
        print("Fraud alerts consumer stopped.")


if __name__ == "__main__":
    main()
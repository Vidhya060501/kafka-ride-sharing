# rider-service/consumers/ride_requests_consumer.py

from confluent_kafka import Consumer, KafkaException
import json


def create_consumer():
    config = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "ride-requests-logger",   # consumer group id
        "auto.offset.reset": "earliest",      # start from beginning if no offset
    }
    return Consumer(config)


def main():
    consumer = create_consumer()
    topic = "ride-requests"

    consumer.subscribe([topic])
    print(f"👂 Subscribed to topic: {topic}")

    try:
        while True:
            msg = consumer.poll(1.0)  # wait up to 1 second for message
            if msg is None:
                continue

            if msg.error():
                # Some errors are just informational, but for now we'll print them.
                print(f"⚠️ Consumer error: {msg.error()}")
                continue

            key = msg.key().decode("utf-8") if msg.key() else None
            value_str = msg.value().decode("utf-8")

            print("\n📥 Received message:")
            print(f"  topic     : {msg.topic()}")
            print(f"  partition : {msg.partition()}")
            print(f"  offset    : {msg.offset()}")
            print(f"  key       : {key}")
            print(f"  value     : {value_str}")

            # If you want to parse JSON:
            try:
                value = json.loads(value_str)
                print(f"  parsed    : {value}")
            except json.JSONDecodeError:
                print("  (Could not decode JSON)")

    except KeyboardInterrupt:
        print("\n🛑 Stopping consumer...")
    finally:
        consumer.close()
        print("Consumer closed.")


if __name__ == "__main__":
    main()
# analytics-service/payments_avro_consumer.py

from confluent_kafka.avro import AvroConsumer


def create_consumer() -> AvroConsumer:
    config = {
        "bootstrap.servers": "localhost:9092",
        "schema.registry.url": "http://localhost:8081",
        "group.id": "analytics-payments-avro",
        "auto.offset.reset": "earliest",
    }
    return AvroConsumer(config)


def main():
    consumer = create_consumer()
    topic = "payments-avro"
    consumer.subscribe([topic])
    print(f"📊 [Avro] Subscribed to topic: {topic}")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            if msg.error():
                print(f"⚠️ Consumer error: {msg.error()}")
                continue

            key = msg.key().decode("utf-8") if msg.key() else None
            value = msg.value()  # Already deserialized to dict

            print("\n📥 [Avro] Payment event:")
            print(f"  key   : {key}")
            print(f"  value : {value}")

    except KeyboardInterrupt:
        print("\n🛑 Stopping Avro payments consumer...")
    finally:
        consumer.close()
        print("Avro consumer stopped.")


if __name__ == "__main__":
    main()
# pricing-service/pricing_service.py

from confluent_kafka import Consumer, Producer
from pydantic import BaseModel
from typing import Dict
import json
import time
import math


# ---------- Models ----------

class RideRequest(BaseModel):
    rider_id: str
    pickup_lat: float
    pickup_lon: float
    destination_lat: float
    destination_lon: float
    ride_type: str
    timestamp: int


class DriverLocationEvent(BaseModel):
    driver_id: str
    lat: float
    lon: float
    availability: str  # "AVAILABLE" or "BUSY"
    timestamp: int


class PricingUpdate(BaseModel):
    zone_id: str
    surge_multiplier: float
    active_drivers: int
    recent_demand: float
    timestamp: int


# ---------- Zone utilities ----------

def compute_zone(lat: float, lon: float, size: float = 0.01) -> str:
    """
    Compute a simple grid-based zone id from latitude/longitude.
    Must match matching-engine's compute_zone.
    """
    lat_idx = int(lat / size)
    lon_idx = int(lon / size)
    return f"{lat_idx}:{lon_idx}"


# ---------- Kafka setup ----------

def create_consumer() -> Consumer:
    config = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "pricing-service",
        "auto.offset.reset": "earliest",
    }
    return Consumer(config)


def create_producer() -> Producer:
    config = {
        "bootstrap.servers": "localhost:9092",
        "linger.ms": 5,
        "batch.num.messages": 1000,
    }
    return Producer(config)


def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Pricing update delivery failed for {msg.key()}: {err}")
    else:
        print(
            f"✅ Pricing update delivered to {msg.topic()} "
            f"[{msg.partition()}] offset {msg.offset()} "
            f"key={msg.key().decode() if msg.key() else None}"
        )


# ---------- Pricing logic ----------

def compute_surge(active_drivers: int, recent_demand: float) -> float:
    """
    Simple surge function based on demand/supply.
    """
    if recent_demand <= 0:
        return 1.0

    if active_drivers == 0:
        # No drivers, some demand → heavy surge
        return 2.5

    load_factor = recent_demand / max(active_drivers, 1)
    surge = 1.0 + min(load_factor * 0.3, 2.0)  # cap contribution
    surge = max(1.0, min(surge, 3.0))          # clamp to [1.0, 3.0]
    return round(surge, 2)


def main():
    consumer = create_consumer()
    producer = create_producer()

    topics = ["ride-requests", "driver-locations"]
    consumer.subscribe(topics)
    print(f"💰 Pricing Service subscribed to: {topics}")

    # State
    drivers_in_zone: Dict[str, int] = {}       # zone_id -> count of AVAILABLE drivers
    driver_zone: Dict[str, str] = {}           # driver_id -> current zone_id
    driver_available: Dict[str, bool] = {}     # driver_id -> whether counted as AVAILABLE
    recent_demand: Dict[str, float] = {}       # zone_id -> demand score
    last_decay_time = time.time()

    try:
        while True:
            msg = consumer.poll(0.5)
            if msg is None:
                # Periodically decay demand
                now = time.time()
                if now - last_decay_time > 5.0:
                    decay_factor = 0.9
                    for z in list(recent_demand.keys()):
                        recent_demand[z] *= decay_factor
                        if recent_demand[z] < 0.1:
                            del recent_demand[z]
                    last_decay_time = now
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

            # ---------- driver-locations ----------
            if topic == "driver-locations":
                try:
                    event = DriverLocationEvent(**payload)
                except Exception as e:
                    print(f"❌ Failed to parse driver-locations: {e}")
                    continue

                new_zone = compute_zone(event.lat, event.lon)
                driver_id = event.driver_id
                new_available = (event.availability == "AVAILABLE")

                old_zone = driver_zone.get(driver_id)
                old_available = driver_available.get(driver_id, False)

                # If previously counted as available in some zone, decrement that zone
                if old_available and old_zone is not None:
                    drivers_in_zone[old_zone] = max(drivers_in_zone.get(old_zone, 1) - 1, 0)

                # Update driver state
                driver_zone[driver_id] = new_zone
                driver_available[driver_id] = new_available

                # If now available, increment new zone
                if new_available:
                    drivers_in_zone[new_zone] = drivers_in_zone.get(new_zone, 0) + 1

                active = drivers_in_zone.get(new_zone, 0)
                demand = recent_demand.get(new_zone, 0.0)
                surge = compute_surge(active, demand)

                update = PricingUpdate(
                    zone_id=new_zone,
                    surge_multiplier=surge,
                    active_drivers=active,
                    recent_demand=round(demand, 2),
                    timestamp=now_ms,
                )
                out_topic = "pricing-updates"
                producer.produce(
                    topic=out_topic,
                    key=new_zone.encode("utf-8"),
                    value=update.model_dump_json().encode("utf-8"),
                    on_delivery=delivery_report,
                )
                producer.poll(0)

                print(f"📤 Pricing update (driver-locations): {update}")

            # ---------- ride-requests ----------
            elif topic == "ride-requests":
                try:
                    req = RideRequest(**payload)
                except Exception as e:
                    print(f"❌ Failed to parse ride-request: {e}")
                    continue

                zone = compute_zone(req.pickup_lat, req.pickup_lon)

                # Increase demand score for this pickup zone
                recent_demand[zone] = recent_demand.get(zone, 0.0) + 1.0

                active = drivers_in_zone.get(zone, 0)
                demand = recent_demand.get(zone, 0.0)
                surge = compute_surge(active, demand)

                update = PricingUpdate(
                    zone_id=zone,
                    surge_multiplier=surge,
                    active_drivers=active,
                    recent_demand=round(demand, 2),
                    timestamp=now_ms,
                )
                out_topic = "pricing-updates"
                producer.produce(
                    topic=out_topic,
                    key=zone.encode("utf-8"),
                    value=update.model_dump_json().encode("utf-8"),
                    on_delivery=delivery_report,
                )
                producer.poll(0)

                print(f"📤 Pricing update (ride-requests): {update}")

    except KeyboardInterrupt:
        print("\n🛑 Stopping Pricing Service...")
    finally:
        print("Flushing producer...")
        producer.flush()
        consumer.close()
        print("Pricing Service stopped.")


if __name__ == "__main__":
    main()
# matching-engine/matching_engine.py

from confluent_kafka import Consumer, Producer
from pydantic import BaseModel
from typing import Dict, Optional
import json
import math
import time


# ---------- Models ----------

class DriverState(BaseModel):
    driver_id: str
    lat: float
    lon: float
    availability: str  # "AVAILABLE" or "BUSY"
    updated_at: int    # epoch millis


class RideRequest(BaseModel):
    rider_id: str
    pickup_lat: float
    pickup_lon: float
    destination_lat: float
    destination_lon: float
    ride_type: str
    timestamp: int


class PricingUpdate(BaseModel):
    zone_id: str
    surge_multiplier: float
    active_drivers: int
    recent_demand: float
    timestamp: int


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
    surge_multiplier: float  # 👈 new field


# ---------- Zone utilities (same logic as pricing-service) ----------

def compute_zone(lat: float, lon: float, size: float = 0.01) -> str:
    """
    Compute a simple grid-based zone id from latitude/longitude.
    Must match the logic used in pricing-service.
    """
    lat_idx = int(lat / size)
    lon_idx = int(lon / size)
    return f"{lat_idx}:{lon_idx}"


# ---------- Kafka setup ----------

def create_consumer() -> Consumer:
    config = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "matching-engine",
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
    }
    return Producer(config)


def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Match delivery failed for {msg.key()}: {err}")
    else:
        print(
            f"✅ Match delivered to {msg.topic()} "
            f"[{msg.partition()}] offset {msg.offset()} key={msg.key().decode()}"
        )


# ---------- Matching logic ----------

def distance(p1_lat: float, p1_lon: float, p2_lat: float, p2_lon: float) -> float:
    """
    Simple Euclidean distance in lat/lon degrees.
    For real systems you'd use haversine, but this is fine for demo.
    """
    return math.sqrt((p1_lat - p2_lat) ** 2 + (p1_lon - p2_lon) ** 2)


def find_nearest_available_driver(
    driver_states: Dict[str, DriverState],
    pickup_lat: float,
    pickup_lon: float,
) -> Optional[DriverState]:
    nearest_driver = None
    nearest_dist = float("inf")

    for driver in driver_states.values():
        if driver.availability != "AVAILABLE":
            continue

        d = distance(pickup_lat, pickup_lon, driver.lat, driver.lon)
        if d < nearest_dist:
            nearest_dist = d
            nearest_driver = driver

    return nearest_driver


# ---------- Main loop ----------

def main():
    consumer = create_consumer()
    producer = create_producer()

    topics = ["driver-locations", "ride-requests", "pricing-updates"]
    consumer.subscribe(topics)
    print(f"🧠 Matching Engine subscribed to: {topics}")

    # state store of drivers (like a Kafka Streams KTable, but in-memory)
    driver_states: Dict[str, DriverState] = {}

    # state store of pricing per zone_id
    surge_by_zone: Dict[str, float] = {}

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            if msg.error():
                print(f"⚠️ Consumer error: {msg.error()}")
                continue

            topic = msg.topic()
            key = msg.key().decode("utf-8") if msg.key() else None
            value_str = msg.value().decode("utf-8")

            # ---------- Handle driver-locations ----------
            if topic == "driver-locations":
                try:
                    payload = json.loads(value_str)
                    existing = driver_states.get(payload["driver_id"])

                    # Preserve BUSY status even if location event says AVAILABLE
                    if existing and existing.availability == "BUSY":
                        availability = "BUSY"
                    else:
                        availability = payload.get("availability", "AVAILABLE")

                    driver = DriverState(
                        driver_id=payload["driver_id"],
                        lat=payload["lat"],
                        lon=payload["lon"],
                        availability=availability,
                        updated_at=payload.get("timestamp", int(time.time() * 1000)),
                    )

                    driver_states[driver.driver_id] = driver
                    print(f"🚕 Updated state for {driver.driver_id}: {driver}")
                except Exception as e:
                    print(f"❌ Failed to process driver-locations message: {e}")
                    continue

            # ---------- Handle pricing-updates ----------
            elif topic == "pricing-updates":
                try:
                    payload = json.loads(value_str)
                    update = PricingUpdate(**payload)
                    surge_by_zone[update.zone_id] = update.surge_multiplier

                    print(
                        f"💰 Pricing update for zone={update.zone_id}: "
                        f"surge={update.surge_multiplier}, "
                        f"drivers={update.active_drivers}, "
                        f"demand={update.recent_demand}"
                    )
                except Exception as e:
                    print(f"❌ Failed to process pricing-updates message: {e}")
                    continue

            # ---------- Handle ride-requests ----------
            elif topic == "ride-requests":
                try:
                    payload = json.loads(value_str)
                    ride = RideRequest(
                        rider_id=payload["rider_id"],
                        pickup_lat=payload["pickup_lat"],
                        pickup_lon=payload["pickup_lon"],
                        destination_lat=payload["destination_lat"],
                        destination_lon=payload["destination_lon"],
                        ride_type=payload["ride_type"],
                        timestamp=payload["timestamp"],
                    )
                    print(f"\n🎯 New ride request: {ride}")

                    # Find nearest driver
                    driver = find_nearest_available_driver(
                        driver_states,
                        ride.pickup_lat,
                        ride.pickup_lon,
                    )

                    if driver is None:
                        print("❌ No AVAILABLE drivers found for this request.")
                        # In real system, produce a 'no-match' or 'queue' event.
                        continue

                    # Mark driver as BUSY in our state
                    driver.availability = "BUSY"
                    driver_states[driver.driver_id] = driver

                    # Determine pricing zone & surge
                    zone = compute_zone(ride.pickup_lat, ride.pickup_lon)
                    surge_multiplier = surge_by_zone.get(zone, 1.0)

                    now = int(time.time() * 1000)
                    match_id = f"{ride.rider_id}-{driver.driver_id}-{now}"

                    # Naive ETA: distance * constant factor
                    dist = distance(
                        ride.pickup_lat, ride.pickup_lon, driver.lat, driver.lon
                    )
                    eta_seconds = int(dist * 1000)  # totally fake, just demo

                    match = RideMatch(
                        match_id=match_id,
                        rider_id=ride.rider_id,
                        driver_id=driver.driver_id,
                        pickup_lat=ride.pickup_lat,
                        pickup_lon=ride.pickup_lon,
                        driver_lat=driver.lat,
                        driver_lon=driver.lon,
                        estimated_eta_seconds=eta_seconds,
                        timestamp=now,
                        surge_multiplier=surge_multiplier,
                    )

                    match_topic = "ride-matches"
                    match_key = driver.driver_id  # 👈 partition by driver

                    producer.produce(
                        topic=match_topic,
                        key=match_key.encode("utf-8"),
                        value=match.model_dump_json().encode("utf-8"),
                        on_delivery=delivery_report,
                    )
                    producer.poll(0)

                    print(
                        f"✅ Created match: {match} "
                        f"(zone={zone}, surge={surge_multiplier})"
                    )

                except Exception as e:
                    print(f"❌ Failed to process ride-requests message: {e}")
                    continue

    except KeyboardInterrupt:
        print("\n🛑 Stopping Matching Engine...")
    finally:
        print("Flushing producer...")
        producer.flush()
        consumer.close()
        print("Matching Engine stopped.")


if __name__ == "__main__":
    main()
# rider-service/app.py

from fastapi import FastAPI
from fastapi import HTTPException
from models import RideRequest
from kafka_producer import RideRequestProducer

app = FastAPI(title="Rider Service")

producer = RideRequestProducer(bootstrap_servers="localhost:9092")


@app.post("/api/rides/request")
async def request_ride(ride: RideRequest):
    try:
        # ensure server-side timestamp if not provided
        ride.ensure_timestamp()

        producer.send_ride_request(ride.dict())
        return {
            "status": "OK",
            "message": "Ride request submitted",
            "rider_id": ride.rider_id,
            "timestamp": ride.timestamp,
        }
    except Exception as e:
        # In real code, log the exception with logging framework
        raise HTTPException(status_code=500, detail=str(e))


@app.on_event("shutdown")
def shutdown_event():
    # Ensure all messages are flushed on shutdown
    producer.flush()
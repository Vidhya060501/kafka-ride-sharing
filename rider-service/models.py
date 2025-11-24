# rider-service/models.py

from pydantic import BaseModel, Field
from typing import Literal
import time


class RideRequest(BaseModel):
    rider_id: str = Field(..., description="Unique rider ID")
    pickup_lat: float
    pickup_lon: float
    destination_lat: float
    destination_lon: float
    ride_type: Literal["POOL", "PREMIUM", "STANDARD"] = "STANDARD"
    timestamp: int | None = Field(
        default=None,
        description="Epoch millis. If not provided, server will set it."
    )

    def ensure_timestamp(self):
        if self.timestamp is None:
            self.timestamp = int(time.time() * 1000)
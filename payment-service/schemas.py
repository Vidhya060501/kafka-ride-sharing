# payment-service/schemas.py
from confluent_kafka import avro

payment_value_schema_str = """
{
  "namespace": "com.rideshare.payments",
  "type": "record",
  "name": "PaymentEvent",
  "fields": [
    {"name": "payment_id",        "type": "string"},
    {"name": "trip_id",           "type": "string"},
    {"name": "rider_id",          "type": "string"},
    {"name": "driver_id",         "type": "string"},
    {"name": "amount",            "type": "double"},
    {"name": "currency",          "type": "string"},
    {"name": "status",            "type": "string"},
    {"name": "timestamp",         "type": "long"},
    {"name": "base_fare",         "type": "double"},
    {"name": "surge_multiplier",  "type": "double"}
  ]
}
"""

payment_value_schema = avro.loads(payment_value_schema_str)
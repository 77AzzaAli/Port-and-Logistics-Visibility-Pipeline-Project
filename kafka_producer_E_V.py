from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime

print("=" * 60)
print("🚢 PORT & CUSTOMS EVENT PRODUCER STARTED")
print("=" * 60)

# Kafka connection
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    max_block_ms=5000
)

message_count = 0

# Sample realistic data
ports = ["Alexandria", "Port Said", "Damietta"]
containers = [f"CONT_{i}" for i in range(100, 200)]
shipments = [f"SHIP_{i}" for i in range(500, 600)]
trucks = [f"TRUCK_{i}" for i in range(1000, 1100)]

# Event flows
event_flow = [
    "VESSEL_ARRIVED",
    "CONTAINER_DISCHARGED",
    "YARD_ENTER",
    "YARD_POSITION_ASSIGNED",
    "CUSTOMS_SUBMITTED",
    "CUSTOMS_CLEAR",
    "GATE_OUT",
    "OUT_FOR_DELIVERY",
    "DELIVERED"
]


def generate_event():
    global message_count
    message_count += 1

    event_type = random.choice(event_flow)

    event = {
        "event_id": message_count,
        
        "event_type": event_type,
        "container_id": random.choice(containers),
        "shipment_id": random.choice(shipments),
        "port": random.choice(ports),
        "timestamp": datetime.now().isoformat()
    }

    # Event-specific data
    if event_type == "VESSEL_ARRIVED":
        event["status"] = random.choice(["BerthingCompleted", "PilotBoarded", "VesselMoored"])

    elif event_type == "CONTAINER_DISCHARGED":
        event["crane_id"] = f"CRANE_{random.randint(1,5)}"
        event["status"] = random.choice(["Lifted", "Weighed", "Damaged", "Retry"])
        event["delay_seconds"] = random.randint(0, 300)

    elif event_type == "YARD_ENTER":
        event["gate_status"] = random.choice(["PASSED", "FAILED"])
        event["ocr_valid"] = random.choice([True, False])

    elif event_type == "YARD_POSITION_ASSIGNED":
        event["slot"] = f"YARD_{random.randint(1,50)}"
        event["optimized"] = random.choice([True, False])
        event["congestion"] = random.choice([True, False])

    elif event_type == "CUSTOMS_SUBMITTED":
        event["documents"] = random.choice(["COMPLETE", "MISSING"])

    elif event_type == "CUSTOMS_CLEAR":
        event["risk_score"] = round(random.uniform(0, 1), 2)
        event["inspection"] = random.choice(["REQUIRED", "NOT_REQUIRED"])
        event["status"] = random.choice(["CLEARED", "HOLD"])

    elif event_type == "GATE_OUT":
        event["truck_id"] = random.choice(trucks)
        event["payment_status"] = random.choice(["PAID", "PENDING"])
        event["congestion"] = random.choice([True, False])

    elif event_type == "OUT_FOR_DELIVERY":
        event["truck_id"] = random.choice(trucks)
        event["route"] = f"Route_{random.randint(1,10)}"
        event["delay"] = random.choice([True, False])

    elif event_type == "DELIVERED":
        event["delivery_status"] = "CONFIRMED"
        event["receiver"] = random.choice(["Warehouse", "Customer", "Distributor"])

    return event


try:
    while True:
        message = generate_event()

        producer.send("port_events", value=message)
        producer.flush()

        print(
            f"✅ [{message_count}] {message['event_type']} | "
            f"{message['container_id']} | {message['shipment_id']}"
        )

        time.sleep(2)

except KeyboardInterrupt:
    print(f"\n⏹️ Stopped. Total events sent: {message_count}")
    producer.close()

except Exception as e:
    print(f"❌ Error: {e}")
    producer.close()
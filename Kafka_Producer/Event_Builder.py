import uuid
import random
from datetime import datetime, timedelta

from Config import *
from Simulator import system_pressure

# =========================================================
# GLOBAL MESSAGE COUNTER
# =========================================================
msg_count = 0

# =========================================================
# EVENT BUILDER
# =========================================================
def build_event(cid, stage, c, status, reason):

    global msg_count
    msg_count += 1

    # =====================================================
    # STAGE DURATION MODEL
    # =====================================================
    planned = random.randint(
        *STAGE_MODEL.get(stage, (1, 2, 0.1))[:2]
    )

    actual = planned

    # =====================================================
    # CONGESTION IMPACT
    # =====================================================
    congestion_factor = 1 + (system_pressure / 200)

    # =====================================================
    # DELAY / FAILURE IMPACT
    # =====================================================
    if status == "DELAYED":
        actual += int(random.randint(6, 72) * congestion_factor)

    elif status == "FAILED":
        actual += random.randint(1, 6)

    elif status == "HOLD":
        actual += random.randint(6, 24)

    # =====================================================
    # OPTION 2 — CORRECT INDUSTRY TIME MODEL
    # =====================================================

    # 1. BUSINESS EVENT TIME (what actually happened in port)
    event_time = c["event_time"] + timedelta(hours=actual)

    # persist timeline
    c["event_time"] = event_time

    # 2. NETWORK LATENCY (Kafka delay simulation)
    network_delay_seconds = random.randint(1, 90)

    # 3. INGESTION TIME (Kafka receives AFTER event)
    ingestion_time = event_time + timedelta(seconds=network_delay_seconds)

    # 4. PRODUCER GENERATION TIME (same as ingestion moment here)
    generated_at = ingestion_time

    # =====================================================
    # TERMINAL EVENT HANDLING
    # =====================================================
    if stage in TERMINAL_STATES:

        return {
            "schema_version": SCHEMA_VERSION,
            "event_id": str(uuid.uuid4()),
            "sequence_id": msg_count,
            "container_id": cid,

            "event_type": stage,
            "event_category": "COMPLETION",

            # FIXED ORDER (REALISTIC)
            "event_time": event_time.isoformat(),
            "simulation_clock_time": event_time.isoformat(),
            "event_generated_at": generated_at.isoformat(),
            "ingestion_time": ingestion_time.isoformat(),

            "status": "COMPLETED",
            "failure_reason": "NONE"
        }

    # =====================================================
    # NORMAL EVENT PAYLOAD
    # =====================================================
    return {
        "schema_version": SCHEMA_VERSION,
        "event_id": str(uuid.uuid4()),
        "sequence_id": msg_count,
        "container_id": cid,

        "event_type": stage,
        "event_category": (
            "CUSTOMS"
            if "CUSTOMS" in stage
            else "OPERATIONS"
        ),

        # REALISTIC ORDER
        "event_time": event_time.isoformat(),
        "simulation_clock_time": event_time.isoformat(),
        "event_generated_at": generated_at.isoformat(),
        "ingestion_time": ingestion_time.isoformat(),

        "container_type": c["container_type"],
        "shipping_line": c["shipping_line"],
        "zone": c["zone"],
        "weight": c["weight"],

        "port_id": PORT_ID,
        "vessel_id": VESSEL_ID,

        "planned_duration_hours": planned,
        "actual_duration_hours": actual,
        "delay_hours": max(0, actual - planned),

        "system_pressure": round(system_pressure, 2),

        "status": status,

        "failure_reason": reason if reason else "NONE"
    }
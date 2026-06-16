import random

from itertools import count

from datetime import timedelta

from Config import *

import Simulator

# =========================================================
# GLOBAL SEQUENCE
# =========================================================
message_counter = count(1)

# =========================================================
# GLOBAL STREAM CLOCK
# =========================================================
global_event_clock = SIMULATION_START_TIME

# =========================================================
# BUILD EVENT
# =========================================================
def build_event(
    cid,
    stage,
    container,
    status,
    reason
):

    global global_event_clock

    sequence_id = next(message_counter)

    # =====================================================
    # STAGE MODEL
    # =====================================================
    min_h, max_h, _ = STAGE_MODEL[stage]

    planned_duration = random.randint(
        min_h,
        max_h
    )

    actual_duration = planned_duration

    # =====================================================
    # STATUS IMPACT
    # =====================================================
    if status == "DELAYED":

        actual_duration += random.randint(4, 48)

    elif status == "FAILED":

        actual_duration += random.randint(1, 12)

    elif status == "HOLD":

        actual_duration += random.randint(6, 24)

    # =====================================================
    # EVENT TIME EVOLUTION
    # =====================================================
    previous_time = container["last_event_time"]

    next_time = previous_time + timedelta(
        hours=actual_duration
    )

    # strict stream ordering
    if next_time <= global_event_clock:

        next_time = global_event_clock + timedelta(
            minutes=random.randint(1, 5)
        )

    global_event_clock = next_time

    container["last_event_time"] = next_time

    # =====================================================
    # EVENT
    # =====================================================
    event = {

        "sequence_id": sequence_id,

        "container_id": cid,

        "event_type": stage,

        "event_category": (
            "CUSTOMS"
            if "CUSTOMS" in stage
            else "OPERATIONS"
        ),

        "event_time":
            next_time.isoformat(),

        "container_type":
            container["container_type"],

        "shipping_line":
            container["shipping_line"],

        "zone":
            container["zone"],

        "weight":
            float(container["weight"]),

        "port_name":
            PORT_NAME,

        "planned_duration_hours":
            planned_duration,

        "actual_duration_hours":
            actual_duration,

        "delay_hours":
            max(
                0,
                actual_duration - planned_duration
            ),

        "system_pressure":
            round(
                Simulator.system_pressure,
                2
            ),

        "status":
            status,

        "failure_reason":
            reason,

        "is_delayed":
            status == "DELAYED"
    }

    return event
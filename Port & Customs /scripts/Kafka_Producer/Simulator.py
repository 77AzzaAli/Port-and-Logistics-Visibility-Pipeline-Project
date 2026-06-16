import random

from Config import *

# =========================================================
# GLOBAL STATE
# =========================================================
containers = {}

container_counter = 0

system_pressure = 0

# =========================================================
# CONTAINER CREATION
# =========================================================
def create_container(cid):

    return {

        "step": 0,

        "completed": False,

        "container_type":
            random.choice(CONTAINER_TYPES),

        "shipping_line":
            random.choice(SHIPPING_LINES),

        "zone":
            random.choice(PORT_ZONES),

        "weight":
            round(random.uniform(5000, 30000), 2),

        "last_event_time":
            SIMULATION_START_TIME
    }

# =========================================================
# PRESSURE MODEL
# =========================================================
def update_system_pressure():

    global system_pressure

    active_containers = len(containers)

    pressure = active_containers * PRESSURE_MULTIPLIER

    system_pressure = min(
        pressure,
        MAX_SYSTEM_PRESSURE
    )

# =========================================================
# CONTAINER ID
# =========================================================
def next_container_id():

    global container_counter

    container_counter += 1

    return f"CONT_{container_counter:05d}"

# =========================================================
# EVENT STATE SIMULATION
# =========================================================
def simulate_state(stage):

    r = random.random()

    # FAILED
    if r < FAILURE_PROB:

        return (
            "FAILED",
            random.choice(FAILURES)
        )

    # HOLD
    if r < FAILURE_PROB + HOLD_PROB:

        return (
            "HOLD",
            "WAITING_MANUAL_REVIEW"
        )

    # DELAY
    _, _, delay_prob = STAGE_MODEL[stage]

    congestion_factor = system_pressure / 100

    adjusted_delay = min(
        0.95,
        delay_prob + congestion_factor * 0.30
    )

    if r < FAILURE_PROB + HOLD_PROB + adjusted_delay:

        return (
            "DELAYED",
            random.choice(DELAY_REASONS)
        )

    # SUCCESS
    return (
        "SUCCESS",
        "OPERATION_COMPLETED_SUCCESSFULLY"
    )
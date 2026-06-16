# =========================================================
# CONFIG.py
# CENTRALIZED SYSTEM CONFIGURATION
# =========================================================

from datetime import datetime

# =========================================================
# KAFKA
# =========================================================
KAFKA_BROKER = "kafka:9092"

TOPIC_NAME = "container_events"

# =========================================================
# PORT
# =========================================================
PORT_NAME = "Port of Alexandria"

# =========================================================
# SIMULATION CLOCK
# =========================================================
SIMULATION_START_TIME = datetime.utcnow()

# =========================================================
# SHIPPING
# =========================================================
CONTAINER_TYPES = [
    "20FT",
    "40FT"
]

SHIPPING_LINES = [
    "MAERSK",
    "MSC",
    "CMA_CGM",
    "HAPAG_LLOYD"
]

PORT_ZONES = [
    "BERTH_A",
    "BERTH_B",
    "YARD_1",
    "YARD_2",
    "CUSTOMS_AREA"
]

# =========================================================
# EVENT FLOW
# =========================================================
EVENT_FLOW = [
    "VESSEL_ARRIVED",
    "CONTAINER_DISCHARGED",
    "YARD_ENTER",
    "CUSTOMS_SUBMITTED",
    "CUSTOMS_INSPECTED",
    "CUSTOMS_CLEAR",
    "GATE_OUT",
    "DELIVERED"
]

TERMINAL_STATES = [
    "DELIVERED"
]

# =========================================================
# STAGE MODEL
# (min_hours, max_hours, delay_probability)
# =========================================================
STAGE_MODEL = {

    "VESSEL_ARRIVED": (1, 4, 0.20),

    "CONTAINER_DISCHARGED": (2, 8, 0.30),

    "YARD_ENTER": (4, 24, 0.40),

    "CUSTOMS_SUBMITTED": (12, 72, 0.65),

    "CUSTOMS_INSPECTED": (6, 36, 0.50),

    "CUSTOMS_CLEAR": (2, 12, 0.30),

    "GATE_OUT": (1, 6, 0.15),

    "DELIVERED": (1, 2, 0.00)
}

# =========================================================
# FAILURES
# =========================================================
FAILURE_PROB = 0.05

HOLD_PROB = 0.05

FAILURES = [
    "DOCUMENT_MISSING",
    "CUSTOMS_REJECTED",
    "WEIGHT_MISMATCH",
    "SYSTEM_ERROR",
    "INSPECTION_REQUIRED"
]

# =========================================================
# DELAY REASONS
# =========================================================
DELAY_REASONS = [
    "PORT_CONGESTION",
    "CUSTOMS_QUEUE",
    "DOCUMENT_VERIFICATION",
    "WEATHER_DELAY",
    "EQUIPMENT_SHORTAGE",
    "INSPECTION_BACKLOG"
]

# =========================================================
# SYSTEM PRESSURE
# =========================================================
PRESSURE_MULTIPLIER = 2.5

MAX_SYSTEM_PRESSURE = 100

# =========================================================
# STREAM CONTROL
# =========================================================
MAX_CONTAINERS = 120

NEW_CONTAINER_PROB = 1.40

# =========================================================
# STREAM SPEED
# =========================================================
MIN_SLEEP_SECONDS = 3.0

MAX_SLEEP_SECONDS = 5.0
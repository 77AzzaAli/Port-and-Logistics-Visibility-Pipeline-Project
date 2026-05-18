# =========================================================
# Config.py
# CENTRALIZED SYSTEM CONFIGURATION
# =========================================================

# =========================================================
# KAFKA CONFIGURATION
# =========================================================
KAFKA_BROKER = "kafka:9092"

TOPIC_NAME = "container_events"

# =========================================================
# CORE BUSINESS ENTITIES
# =========================================================
PORT_ID = "PORT_ALEX"

VESSEL_ID = "VES_01"

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
# EVENT LIFECYCLE FLOW
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

# =========================================================
# TERMINAL STATES
# =========================================================
TERMINAL_STATES = [
    "DELIVERED"
]

FINAL_STAGE = "DELIVERED"

# =========================================================
# STAGE SIMULATION MODEL
# (min_hours, max_hours, base_delay_probability)
# =========================================================
STAGE_MODEL = {

    "VESSEL_ARRIVED": (
        1,
        4,
        0.20
    ),

    "CONTAINER_DISCHARGED": (
        2,
        8,
        0.25
    ),

    "YARD_ENTER": (
        4,
        24,
        0.30
    ),

    "CUSTOMS_SUBMITTED": (
        12,
        72,
        0.60
    ),

    "CUSTOMS_INSPECTED": (
        6,
        36,
        0.45
    ),

    "CUSTOMS_CLEAR": (
        2,
        12,
        0.30
    ),

    "GATE_OUT": (
        1,
        6,
        0.15
    )
}

# =========================================================
# FAILURE & HOLD SIMULATION
# =========================================================
FAILURE_PROB = 0.05

HOLD_PROB = 0.05

FAILURES = [
    "DOCUMENT_MISSING",
    "CUSTOMS_REJECTED",
    "INSPECTION_REQUIRED",
    "WEIGHT_MISMATCH",
    "SYSTEM_ERROR"
]

# =========================================================
# STAGE-SPECIFIC DELAY REASONS
# (REALISTIC PORT OPERATIONS MODEL)
# =========================================================
DELAY_REASONS_BY_STAGE = {

    "VESSEL_ARRIVED": [
        "BERTH_UNAVAILABLE",
        "PORT_TRAFFIC_CONGESTION",
        "WEATHER_DELAY"
    ],

    "CONTAINER_DISCHARGED": [
        "CRANE_UNAVAILABLE",
        "EQUIPMENT_FAILURE",
        "SHIFT_DELAY"
    ],

    "YARD_ENTER": [
        "YARD_OVERCAPACITY",
        "TRUCK_QUEUE",
        "GATE_CONGESTION"
    ],

    "CUSTOMS_SUBMITTED": [
        "DOCUMENT_VERIFICATION",
        "CUSTOMS_QUEUE",
        "SYSTEM_REVIEW_DELAY"
    ],

    "CUSTOMS_INSPECTED": [
        "INSPECTION_BACKLOG",
        "HIGH_RISK_FLAG",
        "MANUAL_REVIEW_REQUIRED"
    ],

    "CUSTOMS_CLEAR": [
        "FINAL_APPROVAL_DELAY",
        "COMPLIANCE_CHECK",
        "SYSTEM_VERIFICATION_DELAY"
    ],

    "GATE_OUT": [
        "TRUCK_AVAILABILITY",
        "EXIT_QUEUE",
        "ADMIN_PROCESSING_DELAY"
    ]
}

# =========================================================
# STREAM CONTROL CONFIGURATION
# =========================================================
MAX_CONTAINERS = 120

NEW_CONTAINER_PROB = 0.45

# =========================================================
# EVENT SCHEMA CONFIGURATION
# =========================================================
SCHEMA_VERSION = "1.0"

# =========================================================
# SYSTEM PRESSURE CONFIGURATION
# =========================================================
MAX_SYSTEM_PRESSURE = 100

PRESSURE_MULTIPLIER = 0.8

# =========================================================
# OUT-OF-ORDER EVENT SIMULATION
# =========================================================
OUT_OF_ORDER_PROB = 0.08

MAX_OUT_OF_ORDER_HOURS = 6

# =========================================================
# EVENT TIMING CONFIGURATION
# =========================================================
MIN_SLEEP_SECONDS = 1.0

MAX_SLEEP_SECONDS = 2.5
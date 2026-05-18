import random
import time

from Config import *
from logger import logger

from Kafka_Client import create_producer
from Simulator import (
    containers,
    create_container,
    update_system_pressure,
    simulate_state,
    next_container_id   # ✅ FIXED (no global usage)
)

from Lifecycle import get_stage
from Event_Builder import build_event


print("=" * 80)
print("🚢 PORT & CUSTOMS DIGITAL TWIN - MODULAR VERSION")
print("=" * 80)

# =========================================================
# PRODUCER INIT
# =========================================================
producer = create_producer()

logger.info("Streaming Started")

# =========================================================
# MAIN LOOP
# =========================================================
try:

    while True:

        # update congestion model
        update_system_pressure()

        # shuffle active containers
        ids = list(containers.keys())
        random.shuffle(ids)

        for cid in ids:

            container = containers[cid]

            if container["completed"]:
                continue

            # get stage from lifecycle
            stage = get_stage(container["step"])

            # safety check
            if stage is None:
                container["completed"] = True
                continue

            # =================================================
            # TERMINAL STATE
            # =================================================
            if stage in TERMINAL_STATES:

                event = build_event(
                    cid,
                    stage,
                    container,
                    "COMPLETED",
                    None
                )

                producer.send(TOPIC_NAME, key=cid, value=event)

                logger.info(f"{cid} → {stage} | COMPLETED")

                container["completed"] = True
                continue

            # =================================================
            # STATE SIMULATION
            # =================================================
            status, reason = simulate_state(stage)

            event = build_event(
                cid,
                stage,
                container,
                status,
                reason
            )

            producer.send(TOPIC_NAME, key=cid, value=event)

            logger.info(f"{cid} → {stage} | {status}")

            # move lifecycle forward
            container["step"] += 1

            time.sleep(random.uniform(MIN_SLEEP_SECONDS, MAX_SLEEP_SECONDS))

        # =================================================
        # NEW CONTAINERS INJECTION
        # =================================================
        if len(containers) < MAX_CONTAINERS:

            if random.random() < NEW_CONTAINER_PROB:

                cid = next_container_id()   # ✅ FIXED

                containers[cid] = create_container(cid)

                logger.info(f"NEW CONTAINER: {cid}")

except KeyboardInterrupt:
    logger.warning("Stopped manually")

finally:
    producer.flush()
    producer.close()
    logger.info("Kafka producer closed")
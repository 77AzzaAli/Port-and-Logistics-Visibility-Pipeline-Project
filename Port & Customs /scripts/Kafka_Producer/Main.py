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
    next_container_id
)

from Lifecycle import get_stage

from Event_Builder import build_event

print("=" * 80)
print("PORT & CUSTOMS DIGITAL TWIN")
print("=" * 80)

producer = create_producer()

logger.info("Streaming Started")

try:

    while True:

        update_system_pressure()

        ids = list(containers.keys())

        random.shuffle(ids)

        # =================================================
        # EXISTING CONTAINERS
        # =================================================
        for cid in ids:

            container = containers[cid]

            if container["completed"]:
                continue

            stage = get_stage(container["step"])

            if stage is None:

                container["completed"] = True

                continue

            status, reason = simulate_state(stage)

            event = build_event(
                cid,
                stage,
                container,
                status,
                reason
            )

            producer.send(
                TOPIC_NAME,
                key=cid,
                value=event
            )

            logger.info(
                f"{event['sequence_id']} | "
                f"{cid} | "
                f"{stage} | "
                f"{status}"
            )

            container["step"] += 1

            if stage in TERMINAL_STATES:
                container["completed"] = True

            time.sleep(
                random.uniform(
                    MIN_SLEEP_SECONDS,
                    MAX_SLEEP_SECONDS
                )
            )

        # =================================================
        # NEW CONTAINERS
        # =================================================
        if len(containers) < MAX_CONTAINERS:

            if random.random() < NEW_CONTAINER_PROB:

                cid = next_container_id()

                containers[cid] = create_container(cid)

                logger.info(
                    f"NEW CONTAINER CREATED: {cid}"
                )

except KeyboardInterrupt:

    logger.warning("Stopped manually")

finally:

    producer.flush()

    producer.close()

    logger.info("Kafka Producer Closed")
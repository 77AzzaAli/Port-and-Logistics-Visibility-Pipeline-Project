from Config import EVENT_FLOW


def get_stage(step):

    if step >= len(EVENT_FLOW):
        return None

    return EVENT_FLOW[step]
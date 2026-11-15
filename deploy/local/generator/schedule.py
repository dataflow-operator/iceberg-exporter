"""How long the local generator keeps appending."""


def run_for(duration, interval, now, sleep, step):
    """Call step while now() is before duration, sleeping at most interval."""
    if duration <= 0 or interval <= 0:
        return 0
    deadline = now() + duration
    count = 0
    while now() < deadline:
        step()
        count += 1
        remaining = deadline - now()
        if remaining <= 0:
            break
        sleep(min(interval, remaining))
    return count

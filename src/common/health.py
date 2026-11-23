import time
from pathlib import Path


class Heartbeat:
    """Writes the current epoch second to a file after every poll.

    Kubernetes liveness probe:
        exec: ["sh", "-c", "test $(( $(date +%s) - $(cat /tmp/pipeline-heartbeat) )) -lt 900"]
    Keep the threshold above the worst-case time to process one batch.
    """

    def __init__(self, path: str) -> None:
        self._path = Path(path)

    def beat(self) -> None:
        try:
            self._path.write_text(str(int(time.time())))
        except OSError:
            pass

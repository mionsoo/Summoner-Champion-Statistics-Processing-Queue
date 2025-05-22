from enum import Enum


class JobStatus(Enum):
    Waiting = (0, "waiting")
    Success = (1, "success")
    Working = (2, "working")
    Error   = (3, "error")
    Timeout = (4, "timeout")

    def __init__(self, _type, description):
        self.type = _type
        self.description = description


SUMMONER_QUEUE_THROUGHPUT = 6
MATCH_QUEUE_THROUGHPUT = 6

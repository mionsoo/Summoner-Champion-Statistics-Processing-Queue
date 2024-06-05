from enum import Enum

class Status(Enum):
    Waiting = (0, 'waiting')
    Success = (1, 'success')
    Working = (2, 'working')
    Error   = (3, 'error')

    def __init__(self, code, description):
        self.code = code
        self.description = description

S_EXECUTE_SUMMONER_COUNT = 5
M_EXECUTE_SUMMONER_COUNT = 10

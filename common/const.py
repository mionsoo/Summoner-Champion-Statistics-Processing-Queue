from enum import Enum
class Status(Enum):
    Waiting = (0, 'waiting')
    Success = (1, 'success')
    Working = (2, 'working')
    Error   = (3, 'error')

    def __init__(self, code, description):
        self.code = code
        self.description = description



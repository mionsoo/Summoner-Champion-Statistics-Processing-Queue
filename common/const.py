from enum import Enum
# TODO:
#  아래 문서 참고
#  https://stackoverflow.com/questions/43862184/associating-string-representations-with-an-enum-that-uses-integer-values
class Status(Enum):
    Waiting = (0, 'waiting')
    Success = (1, 'success')
    Working = (2, 'working')
    Error   = (3, 'error')

    def __init__(self, code, description):
        self.code = code
        self.description = description



from enum import Enum
# TODO:
#  아래 문서 참고
#  https://stackoverflow.com/questions/43862184/associating-string-representations-with-an-enum-that-uses-integer-values
class Status(Enum):
    _init_ ='value string'

    Waiting = 0
    Success = 1
    Working = 2
    Error = 3


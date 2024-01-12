from model.summoner_model import WaitingSummonerObj

from collections import deque
from typing import Deque


class QueueComment:
    def __init__(self):
        self.flag = True

    def empty_printed(self):
        self.flag = False

    def print_empty(self):
        self.flag = True

    def is_need_to_print_empty(self):
        return self.flag


class QueueStatus:
    def __init__(self, criterion: int):
        self.status_criterion = criterion
        self.count = 0
        self.deque: Deque[WaitingSummonerObj] = deque()

    def add_count(self):
        self.count += 1

    def sub_count(self):
        self.count -= 1

    def append_left(self, obj: WaitingSummonerObj):
        if obj not in self.deque and obj.status == self.status_criterion:
            self.deque.appendleft(obj)
            self.add_count()

    def pop(self):
        try:
            popped_value = self.deque.pop()
        except IndexError:
            popped_value = WaitingSummonerObj()
        else:
            if popped_value.status == self.status_criterion:
                self.sub_count()

        return popped_value

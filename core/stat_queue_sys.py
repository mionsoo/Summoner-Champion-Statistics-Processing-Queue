from model.summoner_model import WaitingSummonerObj, WaitingSummonerMatchObj
from abc import *

from collections import deque
from typing import Deque
from common.const import Status
from common.db import connect_sql_aurora, RDS_INSTANCE_TYPE, sql_execute_dict



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
        self.deque: Deque[WaitingSummonerObj | WaitingSummonerMatchObj] = deque()

    def add_count(self):
        self.count += 1

    def sub_count(self):
        self.count -= 1

    def append_left(self, obj: WaitingSummonerObj | WaitingSummonerMatchObj):
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

class QueueOperator(metaclass=ABCMeta):
    def __init__(self):
        self.waiting_status = QueueStatus(criterion=Status.Waiting.code)
        self.working_status = QueueStatus(criterion=Status.Working.code)

    def append(self, obj: WaitingSummonerObj | WaitingSummonerMatchObj):
        if obj.status == Status.Waiting.code:
            self.waiting_status.append_left(obj)
        elif obj.status == Status.Working.code:
            self.working_status.append_left(obj)

    def get_current_obj(self) -> WaitingSummonerObj | WaitingSummonerMatchObj | None:
        if self.waiting_status.count >= 1:
            return self.waiting_status.pop()

        elif self.working_status.count >= 1:
            return self.working_status.pop()

        else:
            return None

    def is_all_queue_is_empty(self) -> bool:
        return self.working_status.count == 0 and self.waiting_status.count == 0

    def is_data_exists(self) -> bool:
        return self.working_status.count != 0 or self.waiting_status.count != 0

    @abstractmethod
    def update_new_data(self):
        pass

    @abstractmethod
    def process_job(self, current_obj: WaitingSummonerObj | WaitingSummonerMatchObj):
        pass

    @staticmethod
    @abstractmethod
    def search_suitable_process_func(current_obj: WaitingSummonerObj | WaitingSummonerMatchObj):
        pass
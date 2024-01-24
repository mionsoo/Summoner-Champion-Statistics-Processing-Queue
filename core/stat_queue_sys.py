from core.db_connection import DBConn
from model.summoner_model import WaitingSummonerObj, WaitingSummonerMatchObj
from abc import *

from collections import deque
from typing import Deque, List
from common.const import Status


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

    def reinit(self, objs: List[WaitingSummonerObj | WaitingSummonerMatchObj]):
        self.deque = deque(objs)
        self.count = len(self.deque)

    def add_count(self):
        self.count += 1

    def sub_count(self):
        self.count -= 1

    def extend(self, objs: List[WaitingSummonerObj | WaitingSummonerMatchObj]):
        self.deque.extend(objs)
        self.count += len(objs)

    def append(self, obj: WaitingSummonerObj | WaitingSummonerMatchObj):
        self.deque.append(obj)
        self.add_count()

    def pop(self):
        try:
            popped_value = self.deque.popleft()
        except IndexError:
            popped_value = WaitingSummonerObj()
        else:
            if popped_value.status == self.status_criterion:
                self.sub_count()

        return popped_value


class QueueOperator(metaclass=ABCMeta):
    def __init__(self):
        self.dbconn = DBConn()
        self.waiting_status = QueueStatus(criterion=Status.Waiting.code)
        self.working_status = QueueStatus(criterion=Status.Working.code)
        self.last_obj = None
        self.last_change_status_code = None
        self.ratio = (0.0, 0.0)
        self.is_burst_switch_on = False

        self.dbconn.make_conn()

    def burst_switch_off(self):
        self.is_burst_switch_on = False

    def burst_switch_on(self):
        self.is_burst_switch_on = True

    def calc_total_count(self):
        return self.waiting_status.count + self.working_status.count

    def calc_waiting_ratio(self):
        if self.calc_total_count():
            return self.waiting_status.count / self.calc_total_count()
        else:
            return 0


    def calc_working_ratio(self):
        if self.calc_total_count():
            return self.working_status.count / self.calc_total_count()
        else:
            return 0

    def update_last_obj(self, current_obj: WaitingSummonerObj | WaitingSummonerMatchObj):
        self.last_obj = current_obj

    def update_last_change_status(self, current_change_status_code: int):
        self.last_change_status_code = current_change_status_code

    @abstractmethod
    def get_current_obj(self) -> WaitingSummonerObj | WaitingSummonerMatchObj | None:
        """ Abstract """

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

    @abstractmethod
    def print_remain(self):
        pass
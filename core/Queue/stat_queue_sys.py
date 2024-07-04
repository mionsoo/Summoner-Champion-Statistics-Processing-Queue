import asyncio

from model.summoner_model import WaitingSummonerObj, WaitingSummonerMatchObj
from core.Job.stat_job import JobResult

from abc import *

from collections import deque
from typing import Deque, List
from common.const import Status
from common.utils import get_current_datetime



class QueueEmptyComment:
    def __init__(self):
        self.flag = True

    def set_job_done(self):
        self.flag = False

    def set_job_not_done(self):
        self.flag = True

    def is_set_print(self):
        return self.flag

    async def print_job_ended(self):
        print(f'{get_current_datetime()} | Queue is Empty')
        print('------------------------------\n')
        self.set_job_done()
        await asyncio.sleep(20)


class QueueStatus:
    def __init__(self, queue_type: int):
        self.status_type = queue_type
        self.length = 0
        self.deque: Deque[WaitingSummonerObj | WaitingSummonerMatchObj] = deque()

    async def reinit(self, objs: List[WaitingSummonerObj | WaitingSummonerMatchObj]):
        await asyncio.sleep(0)
        self.deque = deque(objs)
        self.length = len(self.deque)

    async def add_count(self):
        await asyncio.sleep(0)
        self.length += 1

    async def sub_count(self):
        await asyncio.sleep(0)
        self.length -= 1

    async def extend(self, objs: List[WaitingSummonerObj | WaitingSummonerMatchObj]):
        await asyncio.sleep(0)

        self.deque.extend(objs)
        self.length += len(objs)

    async def append(self, obj: WaitingSummonerObj | WaitingSummonerMatchObj):
        await asyncio.sleep(0)

        self.deque.append(obj)
        await self.add_count()

    async def pop(self):
        await asyncio.sleep(0)
        try:
            popped_value = self.deque.popleft()
        except IndexError:
            popped_value = WaitingSummonerObj()
        else:
            if popped_value.status == self.status_type:
                await self.sub_count()

        return popped_value


class QueueOperator(metaclass=ABCMeta):
    def __init__(self):
        self.waiting_queue = QueueStatus(queue_type=Status.Waiting.code)
        self.working_queue = QueueStatus(queue_type=Status.Working.code)
        self.last_obj = None
        self.last_change_status_code = None
        self.ratio = (0.0, 0.0)
        self.is_burst_switch_on = False

    def burst_switch_off(self):
        self.is_burst_switch_on = False

    def burst_switch_on(self):
        self.is_burst_switch_on = True


    def calc_total_count(self):
        return self.waiting_queue.length + self.working_queue.length

    def calc_waiting_ratio(self):
        if self.calc_total_count():
            return self.waiting_queue.length / self.calc_total_count()
        else:
            return 0

    def calc_working_ratio(self):
        if self.calc_total_count():
            return self.working_queue.length / self.calc_total_count()
        else:
            return 0


    def change_last_obj(self, current_obj: WaitingSummonerObj | WaitingSummonerMatchObj):
        self.last_obj = current_obj

    def change_last_status(self, current_change_status_code: int):
        self.last_change_status_code = current_change_status_code


    @abstractmethod
    def get_current_obj(self, pop_count=0) -> WaitingSummonerObj | WaitingSummonerMatchObj | None:
        """ Abstract """

    def is_all_job_done(self) -> bool:
        return self.working_queue.length == 0 and self.waiting_queue.length == 0

    def is_data_exists(self) -> bool:
        return self.working_queue.length != 0 or self.waiting_queue.length != 0


    @abstractmethod
    def update_new_data(self, conn):
        pass

    async def go_back_to_queue(self, job_result: JobResult):
        if job_result.target_obj== Status.Waiting.code:
            await self.waiting_queue.append(job_result.target_obj)

        elif job_result.target_obj == Status.Working.code:
            await self.working_queue.append(job_result.target_obj)

    @abstractmethod
    def print_counts_remain(self, conn=None):
        pass

    @staticmethod
    async def sleep_queue():
        print('queue sleep 20 sec')
        await asyncio.sleep(20)
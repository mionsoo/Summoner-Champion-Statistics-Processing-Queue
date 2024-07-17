import asyncio
from abc import ABC, abstractmethod
from collections import deque
from typing import Deque, List

from common.const import Status
from common.utils import get_current_datetime
from core.Job.stat_job import JobResult
from model.summoner_model import WaitingSummonerMatchObj, WaitingSummonerObj


class QueueEmptyComment:
    def __init__(self):
        self.flag = True

    def set_empty_log_printed(self):
        self.flag = False

    def set_empty_log_not_printed(self):
        self.flag = True

    def is_empty_log_not_printed(self):
        return self.flag

    async def print_empty_log(self):
        print(f"{get_current_datetime()} | Queue is Empty")
        print("------------------------------\n")
        self.set_empty_log_printed()
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


class QueueOperator(ABC):
    def __init__(self):
        self.waiting_queue = QueueStatus(queue_type=Status.Waiting.code)
        self.working_queue = QueueStatus(queue_type=Status.Working.code)
        self.last_obj = None
        self.last_change_status_code = None
        self.ratio = (0.0, 0.0)
        self.is_burst_switch_on = False

    @abstractmethod
    async def update_incoming_data(self, conn):
        """Abstract"""

    @abstractmethod
    def get_current_obj(self, pop_count=0) -> WaitingSummonerObj | WaitingSummonerMatchObj | None:
        """Abstract"""

    @abstractmethod
    def print_counts_remain(self, conn=None):
        """Abstract"""

    @staticmethod
    async def sleep_queue():
        print("queue sleep 20 sec")
        await asyncio.sleep(20)

    def burst_switch_off(self):
        self.is_burst_switch_on = False

    def burst_switch_on(self):
        self.is_burst_switch_on = True

    def calc_total_count(self):
        return self.waiting_queue.length + self.working_queue.length

    def calc_waiting_ratio(self):
        if self.calc_total_count():
            return self.waiting_queue.length / self.calc_total_count()
        return 0

    def calc_working_ratio(self):
        if self.calc_total_count():
            return self.working_queue.length / self.calc_total_count()
        return 0

    def is_all_job_done(self) -> bool:
        return self.working_queue.length == 0 and self.waiting_queue.length == 0

    def is_data_exists(self) -> bool:
        return self.working_queue.length != 0 or self.waiting_queue.length != 0

    async def go_back_to_queue(self, job_result: JobResult):
        if job_result.target_obj == Status.Waiting.code:
            await self.waiting_queue.append(job_result.target_obj)

        elif job_result.target_obj == Status.Working.code:
            await self.working_queue.append(job_result.target_obj)

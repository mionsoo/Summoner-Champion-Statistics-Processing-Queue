import asyncio
from abc import ABC, abstractmethod
from collections import deque
from typing import Deque, List

from common.const import JobStatus
from common.utils import get_current_datetime
from core.Job.stat_job import JobResult
from model.Summoner import WaitingSummonerMatchJob, WaitingSummonerJob


class Comments:
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


class StatQueue:
    def __init__(self, job_type: int):
        self.job_type = job_type
        self.length = 0
        self.deque: Deque[WaitingSummonerJob | WaitingSummonerMatchJob] = deque()

    async def reinit(self, objs: List[WaitingSummonerJob | WaitingSummonerMatchJob]):
        await asyncio.sleep(0)
        self.deque = deque(objs)
        self.length = len(self.deque)

    async def add_count(self):
        await asyncio.sleep(0)
        self.length += 1

    async def sub_count(self):
        await asyncio.sleep(0)
        self.length -= 1

    async def extend(self, objs: List[WaitingSummonerJob | WaitingSummonerMatchJob]):
        await asyncio.sleep(0)

        self.deque.extend(objs)
        self.length += len(objs)

    async def append(self, obj: WaitingSummonerJob | WaitingSummonerMatchJob):
        await asyncio.sleep(0)

        self.deque.append(obj)
        await self.add_count()

    async def pop(self):
        await asyncio.sleep(0)
        try:
            popped_value = self.deque.popleft()
        except IndexError:
            popped_value = WaitingSummonerJob()
        else:
            if popped_value.status == self.job_type:
                await self.sub_count()

        return popped_value


class Operator(ABC):
    def __init__(self):
        self.waiting_queue = StatQueue(job_type=StatQueue.Waiting.type)
        self.working_queue = StatQueue(job_type=StatQueue.Working.type)
        self.last_obj = None
        self.last_change_status_code = None
        self.ratio = (0.0, 0.0)
        self.is_burst_switch_on = False

    @abstractmethod
    async def update_incoming_data(self, conn):
        """Abstract"""

    @abstractmethod
    def get_current_job(self, pop_count=0) -> WaitingSummonerJob | WaitingSummonerMatchJob | None:
        """Abstract"""

    @abstractmethod
    def print_remain_counts(self, conn=None):
        """Abstract"""

    @staticmethod
    async def sleep_queue():
        print("queue sleep 5 sec")
        await asyncio.sleep(5)

    def burst_switch_off(self):
        self.is_burst_switch_on = False

    def burst_switch_on(self):
        self.is_burst_switch_on = True

    def calc_total_job_count(self):
        return self.waiting_queue.length + self.working_queue.length

    def calc_waiting_job_ratio(self):
        if self.calc_total_job_count():
            return self.waiting_queue.length / self.calc_total_job_count()
        return 0

    def calc_working_job_ratio(self):
        if self.calc_total_job_count():
            return self.working_queue.length / self.calc_total_job_count()
        return 0

    def is_all_job_done(self) -> bool:
        return self.working_queue.length == 0 and self.waiting_queue.length == 0

    def is_job_exists(self) -> bool:
        return self.working_queue.length != 0 or self.waiting_queue.length != 0

    async def return_to_queue(self, job_result: JobResult):
        if job_result.target_job == StatQueue.Waiting.type:
            await self.waiting_queue.append(job_result.target_job)

        elif job_result.target_job == StatQueue.Working.type:
            await self.working_queue.append(job_result.target_job)

        elif job_result.target_job == StatQueue.Timeout.type:
            await self.working_queue.append(job_result.target_job)

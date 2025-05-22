import time
from typing import List, Tuple

from common.const import JobStatus
from common.db import execute_select_match_count, execute_select_match_job
from core.Queue.System import Operator, StatQueue
from model.Summoner import WaitingSummonerMatchJob, WaitingSummonerJob


def partition(array:List[WaitingSummonerJob], low, high):
    # choose the rightmost element as pivot
    pivot = array[high].reg_datetime

    # pointer for greater element
    i = low - 1

    # traverse through all elements
    # compare each element with pivot
    for j in range(low, high):
        if array[j].reg_datetime <= pivot:
            # If element smaller than pivot is found
            # swap it with the greater element pointed by i
            i = i + 1

            # Swapping element at i with element at j
            (array[i], array[j]) = (array[j], array[i])

    # Swap the pivot element with the greater element specified by i
    (array[i + 1], array[high]) = (array[high], array[i + 1])

    # Return the position from where partition is done
    return i + 1


def quickSort(array, low, high):
    if low < high:
        # Find pivot element such that
        # element smaller than pivot are on the left
        # element greater than pivot are on the right
        pi = partition(array, low, high)

        # Recursive call on the left of pivot
        quickSort(array, low, pi - 1)

        # Recursive call on the right of pivot
        quickSort(array, pi + 1, high)

# quickSort(data, 0, size - 1)

def allocate_summoner_job(obj: Tuple[str, str]) -> WaitingSummonerJob:
    platform_id, puu_id, reg_date, *_ = obj
    return WaitingSummonerJob(platform_id=platform_id, puu_id=puu_id, status=StatQueue.Working.type, reg_date=reg_date)


class MatchOperator(Operator):
    async def update_incoming_data(self, conn):
        async with conn.cursor() as cursor:
            await self.add_queue(cursor, self.working_queue)

    async def add_queue(self, cursor, stat_queue: StatQueue):
        if stat_queue.status_type == StatQueue.Waiting.type:
            status = StatQueue.Waiting.type
            _status_obj = self.waiting_queue
        else:
            status = StatQueue.Working.type
            _status_obj = self.working_queue

        result = await execute_select_match_job(cursor, status)
        new_jobs = {tuple(allocate_summoner_job(x).__dict__.values()) for x in result}

        exist_jobs = {tuple(x.__dict__.values()) for x in _status_obj.deque}
        dupl_removed_new_jobs = list(map(allocate_summoner_job, new_jobs.difference(exist_jobs)))
        sorted_new_objs = sorted(dupl_removed_new_jobs, key=lambda x: x.reg_datetime)

        if len(exist_jobs) == 0:
            await _status_obj.reinit(sorted_new_objs)
        else:
            await _status_obj.extend(sorted_new_objs)

    async def get_current_job(self, pop_count=0) -> List[WaitingSummonerJob | WaitingSummonerMatchJob | None]:
        if self.waiting_queue.length >= 1:
            return await self.popped_value_n_times(self.waiting_queue, pop_count)

        elif self.working_queue.length >= 1:
            return await self.popped_value_n_times(self.working_queue, pop_count)

        else:
            return [None]

    @staticmethod
    async def popped_value_n_times(
        status_obj: StatQueue, throughput: int
    ) -> List[WaitingSummonerJob | WaitingSummonerMatchJob]:
        if status_obj.length < throughput:
            throughput = throughput - (throughput - status_obj.length)

        return [await status_obj.pop() for _ in range(throughput)]

    async def print_remain_counts(self, conn=None):
        async with conn.cursor() as cursor:
            count = await execute_select_match_count(cursor)

        print(f"\n - Remain\n" f"\tMatch Waiting: {count[0]} ")

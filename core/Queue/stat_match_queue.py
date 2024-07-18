import time
from typing import List, Tuple

from common.const import Status
from common.db import execute_select_match_count, execute_select_match_obj
from core.Queue.stat_queue_sys import QueueOperator, QueueStatus
from model.summoner_model import WaitingSummonerMatchObj, WaitingSummonerObj


def partition(array:List[WaitingSummonerObj], low, high):
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

def wrap_summoner_obj(obj: Tuple[str, str]) -> WaitingSummonerObj:
    platform_id, puu_id, reg_date, *_ = obj
    return WaitingSummonerObj(platform_id=platform_id, puu_id=puu_id, status=Status.Working.code, reg_date=reg_date)


class SummonerMatchQueueOperator(QueueOperator):
    async def update_incoming_data(self, conn):
        async with conn.cursor() as cursor:
            await self.add_queue(cursor, self.working_queue)

    async def add_queue(self, cursor, status_obj: QueueStatus):
        if status_obj.status_type == Status.Waiting.code:
            status = Status.Waiting.code
            _status_obj = self.waiting_queue
        else:
            status = Status.Working.code
            _status_obj = self.working_queue

        result = await execute_select_match_obj(cursor, status)
        new_objs = {tuple(wrap_summoner_obj(x).__dict__.values()) for x in result}

        exist_objs = {tuple(x.__dict__.values()) for x in _status_obj.deque}
        new_objs_removed_dupl = list(map(wrap_summoner_obj, new_objs.difference(exist_objs)))
        sorted_new_objs = sorted(new_objs_removed_dupl, key=lambda x: x.reg_datetime)

        if len(exist_objs) == 0:
            await _status_obj.reinit(sorted_new_objs)
        else:
            await _status_obj.extend(sorted_new_objs)

    async def get_current_obj(self, pop_count=0) -> List[WaitingSummonerObj | WaitingSummonerMatchObj | None]:
        if self.waiting_queue.length >= 1:
            return await self.get_n_time_popped_value(self.waiting_queue, pop_count)

        elif self.working_queue.length >= 1:
            return await self.get_n_time_popped_value(self.working_queue, pop_count)

        else:
            return [None]

    @staticmethod
    async def get_n_time_popped_value(
        status_obj: QueueStatus, pop_count
    ) -> List[WaitingSummonerObj | WaitingSummonerMatchObj]:
        if status_obj.length < pop_count:
            pop_count = pop_count - (pop_count - status_obj.length)

        return [await status_obj.pop() for _ in range(pop_count)]

    async def print_counts_remain(self, conn=None):
        async with conn.cursor() as cursor:
            count = await execute_select_match_count(cursor)

        print(f"\n - Remain\n" f"\tMatch Waiting: {count[0]} ")

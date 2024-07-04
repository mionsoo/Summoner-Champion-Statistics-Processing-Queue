from common.const import Status
from core.Queue.stat_queue_sys import QueueOperator, QueueStatus

from model.summoner_model import WaitingSummonerMatchObj, WaitingSummonerObj
from typing import Tuple, List


def wrap_summoner_obj(obj: Tuple[str, str]) -> WaitingSummonerObj:
    platform_id, puu_id, reg_date, *_ = obj
    return WaitingSummonerObj(
        platform_id=platform_id,
        puu_id=puu_id,
        status=Status.Working.code,
        reg_date=reg_date
    )


def wrap_summoner_match_obj(obj) -> WaitingSummonerMatchObj:
    platform_id, puu_id, status, reg_datetime, match_id = obj
    return WaitingSummonerMatchObj(
        platform_id=platform_id,
        puu_id=puu_id,
        status=status,
        reg_datetime=reg_datetime,
        match_id=match_id
    )


class SummonerMatchQueueOperator(QueueOperator):
    async def update_new_data(self, conn):
        async with conn.cursor() as cursor:
            await self.add_queue(cursor, self.working_queue)

    async def add_queue(self, cursor, status_obj: QueueStatus):
        if status_obj.status_type == Status.Waiting.code:
            status = Status.Waiting.code
            _status_obj = self.waiting_queue
        else:
            status = Status.Working.code
            _status_obj = self.working_queue

        await cursor.execute(
            'SELECT distinct platform_id, puu_id, reg_date '
            'FROM b2c_summoner_match_queue '
            f'WHERE status={status} '
            # f'ORDER BY reg_datetime ASC '
        )
        result = await cursor.fetchall()
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
    async def get_n_time_popped_value(status_obj: QueueStatus, pop_count) -> List[WaitingSummonerObj | WaitingSummonerMatchObj]:
        if status_obj.length < pop_count:
            pop_count = pop_count - (pop_count - status_obj.length)

        return [await status_obj.pop() for _ in range(pop_count)]

    async def print_counts_remain(self, conn=None):
        async with conn.cursor() as cursor:
            await cursor.execute(
                f'SELECT count(*) '
                f'FROM b2c_summoner_match_queue '
                f'WHERE status = {Status.Working.code}'
            )
            count = await cursor.fetchone()
        # conn.close()

        print(f'\n - Remain\n'
              f'\tMatch Waiting: {count[0]} ')

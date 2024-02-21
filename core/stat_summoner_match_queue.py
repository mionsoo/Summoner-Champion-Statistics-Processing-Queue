import asyncio
import traceback

from common.const import Status
from common.utils import get_changed_current_obj_status
from core.stat_queue_sys import QueueOperator, QueueStatus

from helper.stat_summoner_match import wait_func, work_func

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
            await self.add_queue(cursor, self.working_status)

    async def add_queue(self, cursor, status_obj: QueueStatus):
        if status_obj.status_criterion == Status.Waiting.code:
            status = Status.Waiting.code
            _status_obj = self.waiting_status
        else:
            status = Status.Working.code
            _status_obj = self.working_status

        await cursor.execute(
            'SELECT distinct platform_id, puu_id, reg_date '
            'FROM b2c_summoner_match_queue '
            f'WHERE status={status} '
            f'ORDER BY reg_datetime ASC '
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
        if self.waiting_status.count >= 1:
            return await self.get_n_time_popped_value(self.waiting_status, pop_count)

        elif self.working_status.count >= 1:
            return await self.get_n_time_popped_value(self.working_status, pop_count)

        else:
            return [None]

    @staticmethod
    async def get_n_time_popped_value(status_obj: QueueStatus, pop_count) -> List[WaitingSummonerObj | WaitingSummonerMatchObj]:
        if status_obj.count < pop_count:
            pop_count = pop_count - (pop_count - status_obj.count)

        return [await status_obj.pop() for _ in range(pop_count)]

    async def process_job(self, current_obj: WaitingSummonerMatchObj, conn=None, match_ids=list):
        try:
            suitable_func = self.search_suitable_process_func(current_obj)
            queries, func_return = await suitable_func(current_obj, match_ids)
            changed_current_obj_status_code = await get_changed_current_obj_status(current_obj, func_return)

        except Exception:
            changed_current_obj_status_code = Status.Error.code

            if current_obj.status == Status.Waiting.code:
                await self.waiting_status.append(current_obj)
            elif current_obj.status == Status.Working.code:
                await self.working_status.append(current_obj)

            print(traceback.format_exc())

        finally:
            self.update_last_obj(current_obj)
            self.update_last_change_status(changed_current_obj_status_code)
            # return self.update_processed_match_status(changed_current_obj_status_code, current_obj)
            return queries

    @staticmethod
    def update_processed_match_status(changed_current_obj_status_code, current_obj):
        return f'platform_id={repr(current_obj)}'
        # return (
        #     'UPDATE b2c_summoner_match_queue '
        #     f'SET status = {changed_current_obj_status_code} '
        #     f'WHERE platform_id = {repr(current_obj.platform_id)} '
        #     f'and puu_id = {repr(current_obj.puu_id)} '
        #     f'and status = {current_obj.status} '
        # )

    @staticmethod
    def search_suitable_process_func(current_obj: WaitingSummonerMatchObj):
        if current_obj.status == Status.Waiting.code:
            return wait_func
        elif current_obj.status == Status.Working.code:
            return work_func

    async def print_counts_remain(self, conn=None):
        # await asyncio.sleep(0)
        # conn = await connect_sql_aurora_async(RDS_INSTANCE_TYPE.READ)
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

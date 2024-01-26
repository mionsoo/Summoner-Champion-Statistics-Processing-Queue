import asyncio
import time
import traceback

from common.const import Status
from common.db import (
    RDS_INSTANCE_TYPE,
    connect_sql_aurora_async
)
from common.utils import get_changed_current_obj_status, get_current_datetime, logging_time
from core.stat_queue_sys import QueueOperator

from helper.stat_summoner_match import wait_func, work_func

from model.summoner_model import WaitingSummonerMatchObj, WaitingSummonerObj
from typing import Tuple



def wrap_summoner_obj(obj: Tuple[str, str]) -> WaitingSummonerObj:
    platform_id, puu_id = obj
    return WaitingSummonerObj(
        platform_id=platform_id,
        puu_id=puu_id,
        status=Status.Working.code
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
    async def update_new_data(self):
        conn = await connect_sql_aurora_async(RDS_INSTANCE_TYPE.READ)
        async with conn.cursor() as cursor:
            await cursor.execute(
                'SELECT distinct platform_id, puu_id '
                'FROM b2c_summoner_match_queue '
                f'WHERE status={Status.Working.code} '
                f'ORDER BY reg_datetime ASC '
            )

            result = await cursor.fetchall()
            new_working = set(result)

        exist_working = {tuple(x.__dict__.values()) for x in self.working_status.deque}
        new_working_removed_dupl = list(map(wrap_summoner_obj, new_working.difference(exist_working)))

        if len(exist_working) == 0:
            await self.working_status.reinit(sorted(new_working_removed_dupl, key=lambda x: x.reg_datetime))
        else:
            await self.working_status.extend(new_working_removed_dupl)

    async def get_current_obj(self, pop_count=0) -> WaitingSummonerObj | WaitingSummonerMatchObj | None:
        if self.waiting_status.count >= 1:
            return await self.waiting_status.pop()

        elif self.working_status.count >= 1:
            return await self.working_status.pop()

        else:
            return None

    @logging_time
    async def process_job(self, current_obj: WaitingSummonerMatchObj):
        try:
            suitable_func = self.search_suitable_process_func(current_obj)
            func_return = await suitable_func(current_obj)
            changed_current_obj_status_code = await get_changed_current_obj_status(current_obj, func_return)

        except Exception:
            changed_current_obj_status_code = Status.Error.code
            if current_obj.status == Status.Waiting.code:
                await self.waiting_status.append(current_obj)
            elif current_obj.status == Status.Working.code:
                await self.working_status.append(current_obj)
            print(traceback.format_exc())

        finally:
            conn = await connect_sql_aurora_async(RDS_INSTANCE_TYPE.READ)
            async with conn.cursor() as cursor:
                await cursor.execute(
                    'UPDATE b2c_summoner_match_queue '
                    f'SET status = {changed_current_obj_status_code} '
                    f'WHERE platform_id = {repr(current_obj.platform_id)} '
                    f'and puu_id = {repr(current_obj.puu_id)} '
                    f'and status = {current_obj.status} '
                )
                await conn.commit()

            self.update_last_obj(current_obj)
            self.update_last_change_status(changed_current_obj_status_code)


    @staticmethod
    def search_suitable_process_func(current_obj: WaitingSummonerMatchObj):
        if current_obj.status == Status.Waiting.code:
            return wait_func
        elif current_obj.status == Status.Working.code:
            return work_func

    async def print_remain(self):
        await asyncio.sleep(0)
        conn = await connect_sql_aurora_async(RDS_INSTANCE_TYPE.READ)
        async with conn.cursor() as cursor:
            await cursor.execute(
                f'SELECT count(*) '
                f'FROM b2c_summoner_match_queue '
                f'WHERE status = {Status.Working.code}'
            )
            count = await cursor.fetchone()


        print(f'\n - Remain\n'
              f'\tMatch Waiting: {count[0]} ')

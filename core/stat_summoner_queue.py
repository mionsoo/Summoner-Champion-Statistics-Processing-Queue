import asyncio
from datetime import datetime
import time
import traceback

from common.const import Status
from common.db import (
    RDS_INSTANCE_TYPE,
    connect_sql_aurora_async
)
from common.utils import get_changed_current_obj_status, get_current_datetime, logging_time
from core.stat_queue_sys import QueueOperator, QueueStatus

from helper.stat_summoner import wait_func, work_func

from model.summoner_model import WaitingSummonerObj, WaitingSummonerMatchObj

from typing import Callable, Tuple, List


def wrap_summoner_obj(obj: Tuple[str, str, int, datetime]) -> WaitingSummonerObj:
    platform_id, puu_id, status, reg_datetime = obj
    return WaitingSummonerObj(
        platform_id=platform_id,
        puu_id=puu_id,
        status=status,
        reg_datetime=reg_datetime
    )


class SummonerQueueOperator(QueueOperator):
    async def update_new_data(self):
        conn = await connect_sql_aurora_async(RDS_INSTANCE_TYPE.READ)
        async with conn.cursor() as cursor:
            await self.add_queue(cursor, self.waiting_status)
            await self.add_queue(cursor, self.working_status)

        conn.close()

    @staticmethod
    async def add_queue(cursor, status_obj: QueueStatus):
        await cursor.execute(
            'SELECT platform_id, puu_id, status, reg_datetime '
            'FROM b2c_summoner_queue '
            f'WHERE status={Status.Waiting.code if status_obj.status_criterion == Status.Waiting.code else Status.Working.code } '
            f'ORDER BY reg_datetime ASC '
        )
        result = await cursor.fetchall()
        new_objs = set(result)

        exist_objs = {tuple(x.__dict__.values()) for x in status_obj.deque}
        new_objs_removed_dupl = list(map(wrap_summoner_obj, new_objs.difference(exist_objs)))
        sorted_new_objs = list(sorted(new_objs_removed_dupl, key=lambda x: x.reg_datetime))

        if len(exist_objs) == 0:
            await status_obj.reinit(sorted_new_objs)
        else:
            await status_obj.extend(sorted_new_objs)

    async def get_current_obj(self, pop_count=0) -> List[WaitingSummonerObj | WaitingSummonerMatchObj | None]:
        await asyncio.sleep(0)

        if self.is_burst_switch_on and self.calc_working_ratio() < 0.1:
            self.burst_switch_off()

        elif self.is_burst_switch_on:
            return await self.get_n_time_popped_value(self.working_status, pop_count)

        elif not self.is_burst_switch_on and self.calc_working_ratio() > 0.4:
            self.burst_switch_on()

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

    # @logging_time
    async def process_job(self, current_obj: WaitingSummonerObj):
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
                    'UPDATE b2c_summoner_queue '
                    f'SET status = {changed_current_obj_status_code} '
                    f'WHERE platform_id = {repr(current_obj.platform_id)} '
                    f'and puu_id = {repr(current_obj.puu_id)} '
                    f'and status = {current_obj.status} '
                    f'and reg_datetime = "{str(current_obj.reg_datetime)}"'
                )
                await conn.commit()
            conn.close()

            if self.last_obj == current_obj and self.last_change_status_code == changed_current_obj_status_code:
                time.sleep(10)

            self.update_last_obj(current_obj)
            self.update_last_change_status(changed_current_obj_status_code)

    @staticmethod
    def search_suitable_process_func(current_obj: WaitingSummonerObj) -> Callable:
        if current_obj.status == Status.Waiting.code:
            return wait_func
        elif current_obj.status == Status.Working.code:
            return work_func

    def print_remain(self):
        print(f'\n - Remain\n'
              f'\tWaiting: {self.waiting_status.count} ({round(self.calc_waiting_ratio() * 100, 2)}%)\n'
              f'\tWorking: {self.working_status.count} ({round(self.calc_working_ratio() * 100, 2)}%)')
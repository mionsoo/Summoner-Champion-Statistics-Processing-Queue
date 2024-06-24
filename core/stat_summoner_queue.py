import asyncio
from datetime import datetime, date
import traceback
import pytz


from common.const import Status
from common.utils import get_changed_current_obj_status
from core.stat_queue_sys import QueueOperator, QueueStatus

from helper.stat_summoner import wait_func, work_func

from model.summoner_model import WaitingSummonerObj, WaitingSummonerMatchObj

from typing import Callable, Tuple, List


def wrap_summoner_obj(obj: Tuple[str, str, int, date, datetime], season, season_start_timestamp, season_end_timestamp) -> WaitingSummonerObj:
    platform_id, puu_id, reg_date, status, reg_datetime = obj
    return WaitingSummonerObj(
        platform_id=platform_id,
        puu_id=puu_id,
        reg_date=reg_date,
        status=status,
        reg_datetime=reg_datetime,
        season=season,
        season_start_timestamp=season_start_timestamp,
        season_end_timestamp=season_end_timestamp
    )


class SummonerQueueOperator(QueueOperator):
    async def update_new_data(self, conn):
        await self.add_queue(conn, self.waiting_status)
        await self.add_queue(conn, self.working_status)

    async def add_queue(self, conn, status_obj: QueueStatus):
        if status_obj.status_criterion == Status.Waiting.code:
            status = Status.Waiting.code
            _status_obj = self.waiting_status
        else:
            status = Status.Working.code
            _status_obj = self.working_status
        async with conn.cursor() as cursor:

            await cursor.execute(
                'SELECT platform_id, puu_id, reg_date, status, reg_datetime '
                'FROM b2c_summoner_queue '
                f'WHERE status={status} '
                f'ORDER BY reg_datetime ASC '
            )
            result = await cursor.fetchall()
            # new_objs = set(result)

            now = datetime.utcnow()
            KST = pytz.timezone('Asia/Seoul').localize(now).tzinfo
            current_datetime_timestamp = datetime.now(KST).timestamp()
            await cursor.execute(
                "SELECT season, start_timestamp, end_timestamp "
                "FROM b2c_season_info_datetime "
                f"WHERE {int(current_datetime_timestamp)} between start_timestamp and end_timestamp "
                "ORDER BY start_datetime DESC ",
            )
            season, season_start_timestamp, season_end_timestamp = await cursor.fetchone()

        new_objs = {tuple(wrap_summoner_obj(x, season=season, season_start_timestamp=season_start_timestamp, season_end_timestamp=season_end_timestamp).__dict__.values()) for x in result}


        exist_objs = {tuple(x.__dict__.values()) for x in _status_obj.deque}
        new_objs_removed_dupl = list(map(lambda x: wrap_summoner_obj(x[0:5], season=season, season_start_timestamp=season_start_timestamp, season_end_timestamp=season_end_timestamp), new_objs.difference(exist_objs)))
        sorted_new_objs = list(sorted(new_objs_removed_dupl, key=lambda x: x.reg_datetime))

        if len(exist_objs) == 0:
            await _status_obj.reinit(sorted_new_objs)
        else:
            await _status_obj.extend(sorted_new_objs)

    async def get_current_obj(self, pop_count=0) -> List[WaitingSummonerObj | WaitingSummonerMatchObj | None]:
        await asyncio.sleep(0)

        if self.is_burst_switch_on and self.calc_working_ratio() < 0.1:
            self.burst_switch_off()

        elif self.is_burst_switch_on:
            return await self.get_n_time_popped_value(self.working_status, self.waiting_status.count)

        elif not self.is_burst_switch_on and self.calc_working_ratio() >= 0.9:
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

    async def process_job(self, current_obj: WaitingSummonerObj, conn=None, match_ids=None):
        try:
            suitable_func = self.search_suitable_process_func(current_obj)
            func_return = await suitable_func(current_obj, conn)
            changed_current_obj_status_code = await get_changed_current_obj_status(current_obj, func_return)

        except Exception:
            changed_current_obj_status_code = Status.Error.code
            if current_obj.status == Status.Waiting.code:
                await self.waiting_status.append(current_obj)
            elif current_obj.status == Status.Working.code:
                await self.working_status.append(current_obj)
            print(traceback.format_exc())

        finally:
            # if current_obj.status == Status.Working.code and changed_current_obj_status_code == Status.Success.code:
            #     await update_summoner_stat_dynamo(current_obj)
            self.update_last_obj(current_obj)
            self.update_last_change_status(changed_current_obj_status_code)

            # return ('UPDATE b2c_summoner_queue '
            #         f'SET status = {changed_current_obj_status_code} '
            #         f'WHERE platform_id = {repr(current_obj.platform_id)} '
            #         f'and puu_id = {repr(current_obj.puu_id)} '
            #         f'and status = {current_obj.status} '
            #         f'and reg_datetime = "{str(current_obj.reg_datetime)}"')
            return func_return, current_obj, changed_current_obj_status_code

    @staticmethod
    def search_suitable_process_func(current_obj: WaitingSummonerObj) -> Callable:
        if current_obj.status == Status.Waiting.code:
            return wait_func
        elif current_obj.status == Status.Working.code:
            return work_func

    def print_counts_remain(self, conn=None):
        print(f'\n - Remain\n'
              f'\tWaiting: {self.waiting_status.count} ({round(self.calc_waiting_ratio() * 100, 2)}%)\n'
              f'\tWorking: {self.working_status.count} ({round(self.calc_working_ratio() * 100, 2)}%)')

import asyncio
from datetime import date, datetime
from typing import List, Tuple

import pytz

from common.const import Status
from core.Queue.stat_queue_sys import QueueOperator, QueueStatus
from model.summoner_model import WaitingSummonerMatchObj, WaitingSummonerObj


def wrap_summoner_obj(
    obj: Tuple[str, str, int, date, datetime], season, season_start_timestamp, season_end_timestamp
) -> WaitingSummonerObj:
    platform_id, puu_id, reg_date, status, reg_datetime = obj
    return WaitingSummonerObj(
        platform_id=platform_id,
        puu_id=puu_id,
        reg_date=reg_date,
        status=status,
        reg_datetime=reg_datetime,
        season=season,
        season_start_timestamp=season_start_timestamp,
        season_end_timestamp=season_end_timestamp,
    )


class SummonerQueueOperator(QueueOperator):
    async def update_incoming_data(self, conn):
        await self.add_queue(conn, self.waiting_queue)
        await self.add_queue(conn, self.working_queue)

    async def add_queue(self, conn, status_obj: QueueStatus):
        if status_obj.status_type == Status.Waiting.code:
            status = Status.Waiting.code
            _status_obj = self.waiting_queue
        else:
            status = Status.Working.code
            _status_obj = self.working_queue

        async with conn.cursor() as cursor:
            await cursor.execute(
                "SELECT platform_id, puu_id, reg_date, status, reg_datetime "
                "FROM b2c_summoner_queue "
                f"WHERE status={status} "
                f"ORDER BY reg_datetime ASC "
            )
            result = await cursor.fetchall()

            now = datetime.utcnow()
            KST = pytz.timezone("Asia/Seoul").localize(now).tzinfo
            current_datetime_timestamp = datetime.now(KST).timestamp()
            await cursor.execute(
                "SELECT season, start_timestamp, end_timestamp "
                "FROM b2c_season_info_datetime "
                f"WHERE {int(current_datetime_timestamp)} between start_timestamp and end_timestamp "
                "ORDER BY start_datetime DESC ",
            )
            season, season_start_timestamp, season_end_timestamp = await cursor.fetchone()

        new_objs = {
            tuple(
                wrap_summoner_obj(
                    x,
                    season=season,
                    season_start_timestamp=season_start_timestamp,
                    season_end_timestamp=season_end_timestamp,
                ).__dict__.values()
            )
            for x in result
        }

        exist_objs = {tuple(x.__dict__.values()) for x in _status_obj.deque}
        new_objs_removed_dupl = [
            wrap_summoner_obj(
                x[0:5],
                season=season,
                season_start_timestamp=season_start_timestamp,
                season_end_timestamp=season_end_timestamp,
            )
            for x in new_objs.difference(exist_objs)
        ]
        sorted_new_objs = sorted(new_objs_removed_dupl, key=lambda x: x.reg_datetime)

        if len(exist_objs) == 0:
            await _status_obj.reinit(sorted_new_objs)
        else:
            await _status_obj.extend(sorted_new_objs)

    async def get_current_obj(self, pop_count=0) -> List[WaitingSummonerObj | WaitingSummonerMatchObj | None]:
        await asyncio.sleep(0)

        if self.is_burst_switch_on and self.calc_working_ratio() < 0.1:
            self.burst_switch_off()

        elif self.is_burst_switch_on:
            return await self.get_n_time_popped_value(self.working_queue, self.working_queue.length)

        elif not self.is_burst_switch_on and self.calc_working_ratio() >= 1.0:
            self.burst_switch_on()

        if 1 <= self.waiting_queue.length:
            return await self.get_n_time_popped_value(self.waiting_queue, pop_count)

        elif 1 <= self.working_queue.length:
            return await self.get_n_time_popped_value(self.working_queue, pop_count)

        return [None]

    @staticmethod
    async def get_n_time_popped_value(
        status_obj: QueueStatus, pop_count
    ) -> List[WaitingSummonerObj | WaitingSummonerMatchObj]:
        if status_obj.length < pop_count:
            pop_count = pop_count - (pop_count - status_obj.length)

        return [await status_obj.pop() for _ in range(pop_count)]

    def print_counts_remain(self, conn=None):
        print(
            f"\n - Remain\n"
            f"\tWaiting: {self.waiting_queue.length} ({round(self.calc_waiting_ratio() * 100, 2)}%)\n"
            f"\tWorking: {self.working_queue.length} ({round(self.calc_working_ratio() * 100, 2)}%)"
        )

import asyncio
from datetime import date, datetime
from typing import List, Tuple

import pytz

from common.const import JobStatus
from core.Queue.System import Operator, StatQueue
from model.Summoner import WaitingSummonerMatchJob, WaitingSummonerJob


def allocate_summoner_job(
    obj: Tuple[str, str, int, date, datetime], season, season_start_timestamp, season_end_timestamp
) -> WaitingSummonerJob:
    platform_id, puu_id, reg_date, status, reg_datetime = obj
    return WaitingSummonerJob(
        platform_id=platform_id,
        puu_id=puu_id,
        reg_date=reg_date,
        status=status,
        reg_datetime=reg_datetime,
        season=season,
        season_start_timestamp=season_start_timestamp,
        season_end_timestamp=season_end_timestamp,
    )


class SummonerOperator(Operator):
    async def update_incoming_data(self, conn):
        await self.add_queue(conn, self.waiting_queue)
        await self.add_queue(conn, self.working_queue)

    async def add_queue(self, conn, status_obj: StatQueue):
        if status_obj.status_type == StatQueue.Waiting.type:
            status_type = StatQueue.Waiting.type
            _current_queue = self.waiting_queue
        else:
            status_type = StatQueue.Working.type
            _current_queue = self.working_queue

        async with conn.cursor() as cursor:
            await cursor.execute(
                "SELECT platform_id, puu_id, reg_date, status, reg_datetime "
                "FROM b2c_summoner_queue "
                f"WHERE status={status_type} "
                f"ORDER BY reg_datetime ASC "
            )
            result = await cursor.fetchall()

            KST = pytz.timezone("Asia/Seoul").localize(datetime.utcnow()).tzinfo
            current_datetime_timestamp = datetime.now(KST).timestamp()
            await cursor.execute(
                "SELECT season, start_timestamp, end_timestamp "
                "FROM b2c_season_info_datetime "
                f"WHERE {int(current_datetime_timestamp)} between start_timestamp and end_timestamp "
                "ORDER BY start_datetime DESC ",
            )
            season, season_start_timestamp, season_end_timestamp = await cursor.fetchone()

        new_jobs = {
            tuple(
                allocate_summoner_job(
                    summoner_info,
                    season=season,
                    season_start_timestamp=season_start_timestamp,
                    season_end_timestamp=season_end_timestamp,
                ).__dict__.values()
            )
            for summoner_info in result
        }

        exist_jobs = {tuple(x.__dict__.values()) for x in _current_queue.deque}
        dupl_removed_new_jobs = [
            allocate_summoner_job(
                x[0:5],
                season=season,
                season_start_timestamp=season_start_timestamp,
                season_end_timestamp=season_end_timestamp,
            )
            for x in new_jobs.difference(exist_jobs)
        ]
        sorted_new_jobs = sorted(dupl_removed_new_jobs, key=lambda summoner: summoner.reg_datetime)

        if len(exist_jobs):
            await _current_queue.extend(sorted_new_jobs)
        else:
            await _current_queue.reinit(sorted_new_jobs)

    async def get_current_job(self, throughput=0) -> List[WaitingSummonerJob | WaitingSummonerMatchJob | None]:
        await asyncio.sleep(0)

        if self.is_burst_switch_on:
            return await self.popped_value_n_times(self.working_queue, self.working_queue.length)

        if 1 <= self.waiting_queue.length:
            return await self.popped_value_n_times(self.waiting_queue, throughput)

        elif 1 <= self.working_queue.length:
            return await self.popped_value_n_times(self.working_queue, throughput)

        return [None]

    async def check_burst_switch_on_off(self):
        if self.is_burst_switch_on and self.calc_working_job_ratio() < 0.1:
            self.burst_switch_off()
        elif not self.is_burst_switch_on and self.calc_working_job_ratio() >= 1.0:
            self.burst_switch_on()




    @staticmethod
    async def popped_value_n_times(
        status_obj: StatQueue, pop_count
    ) -> List[WaitingSummonerJob | WaitingSummonerMatchJob]:
        if status_obj.length < pop_count:
            pop_count = pop_count - (pop_count - status_obj.length)

        return [await status_obj.pop() for _ in range(pop_count)]

    def print_remain_counts(self, conn=None):
        print(
            f"\n - Remain\n"
            f"\tWaiting: {self.waiting_queue.length} ({round(self.calc_waiting_job_ratio() * 100, 2)}%)\n"
            f"\tWorking: {self.working_queue.length} ({round(self.calc_working_job_ratio() * 100, 2)}%)"
        )

import asyncio
import traceback
import time
from common.const import Status
from common.db import connect_sql_aurora, RDS_INSTANCE_TYPE, sql_execute_dict, sql_execute
from core.stat_queue_sys import QueueStatus, QueueOperator
from helper.stat_summoner import wait_func, work_func
from common.utils import change_current_obj_status
from model.summoner_model import WaitingSummonerObj
from typing import Callable

def test(obj):
    platform_id, puu_id, status, reg_datetime = obj
    return WaitingSummonerObj(
        platform_id=platform_id,
        puu_id = puu_id,
        status = status,
        reg_datetime = reg_datetime
    )




class SummonerQueueOperator(QueueOperator):

    async def update_new_data(self):

        with connect_sql_aurora(RDS_INSTANCE_TYPE.READ) as conn:
            waiting = set(sql_execute(
                'SELECT platform_id, puu_id, status, reg_datetime '
                'from b2c_summoner_queue '
                f'WHERE status={Status.Waiting.code} '
                f'order by reg_datetime desc',
                conn)
            )
            working = set(sql_execute(
                'SELECT platform_id, puu_id, status, reg_datetime '
                'from b2c_summoner_queue '
                f'WHERE status={Status.Working.code} '
                f'order by reg_datetime desc',
                conn)
            )

        waiting_q = set(map(lambda x: tuple(x.__dict__.values()), self.waiting_status.deque))
        q = waiting.difference(waiting_q)

        working_q = set(map(lambda x: tuple(x.__dict__.values()), self.working_status.deque))
        e = working.difference(working_q)
        v = q | e

        tasks = [asyncio.create_task(self.append(test(i)))for i in v]
        await asyncio.gather(*tasks)


    def process_job(self, current_obj: WaitingSummonerObj):
        try:
            suitable_func = self.search_suitable_process_func(current_obj)
            func_return = suitable_func(current_obj)
            changed_current_obj_status_code = change_current_obj_status(current_obj, func_return)

        except Exception:
            changed_current_obj_status_code = Status.Error.code
            self.append(current_obj)
            print(traceback.format_exc())

        finally:
            with connect_sql_aurora(RDS_INSTANCE_TYPE.READ) as conn:
                sql_execute_dict(
                    'UPDATE b2c_summoner_queue '
                    f'SET status = {changed_current_obj_status_code} '
                    f'WHERE platform_id = {repr(current_obj.platform_id)} '
                    f'and puu_id = {repr(current_obj.puu_id)} '
                    f'and status = {current_obj.status} '
                    f'and reg_datetime = "{str(current_obj.reg_datetime)}"',
                    conn
                )
                conn.commit()

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

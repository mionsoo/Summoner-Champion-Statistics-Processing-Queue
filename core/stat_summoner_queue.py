import asyncio
from datetime import datetime
import time
import traceback

from common.const import Status
from common.db import (
    connect_sql_aurora,
    RDS_INSTANCE_TYPE,
    sql_execute,
    sql_execute_dict,
)
from common.utils import change_current_obj_status
from core.stat_queue_sys import QueueOperator

from helper.stat_summoner import wait_func, work_func

from model.summoner_model import WaitingSummonerObj

from typing import Callable, Tuple


def wrap_summoner_obj(obj: Tuple[str, str, int, datetime]) -> WaitingSummonerObj:
    platform_id, puu_id, status, reg_datetime = obj
    return WaitingSummonerObj(
        platform_id=platform_id,
        puu_id=puu_id,
        status=status,
        reg_datetime=reg_datetime
    )


class SummonerQueueOperator(QueueOperator):
    def update_new_data(self):

        with connect_sql_aurora(RDS_INSTANCE_TYPE.READ) as conn:
            s = time.time()
            new_waiting = set(sql_execute(
                'SELECT platform_id, puu_id, status, reg_datetime '
                'from b2c_summoner_queue '
                f'WHERE status={Status.Waiting.code} '
                f'order by reg_datetime desc',
                conn)
            )
            print(f'{round(time.time() - s, 4)}  db waiting call')

            s = time.time()
            new_working = set(sql_execute(
                'SELECT platform_id, puu_id, status, reg_datetime '
                'from b2c_summoner_queue '
                f'WHERE status={Status.Working.code} '
                f'order by reg_datetime desc',
                conn)
            )
            print(f'{round(time.time() - s, 4)}  db working call')


        s = time.time()
        exist_waiting = {tuple(x.__dict__.values()) for x in self.waiting_status.deque}
        print(f'{round(time.time() - s, 4)}  make_set')

        s = time.time()
        new_waiting_removed_dupl = new_waiting.difference(exist_waiting)
        print(f'{round(time.time() - s, 4)}  remove duplicate')

        s = time.time()
        exist_working = {tuple(x.__dict__.values()) for x in self.working_status.deque}
        print(f'{round(time.time() - s, 4)}  make_set2')

        s = time.time()
        new_working_removed_dupl = new_working.difference(exist_working)
        print(f'{round(time.time() - s, 4)}  remove duplicate2')

        s = time.time()
        new_summoner = new_waiting_removed_dupl | new_working_removed_dupl
        print(f'{round(time.time() - s, 4)}  concat')

        s = time.time()
        # tasks = [asyncio.create_task(self.append(wrap_summoner_obj(summoner))) for summoner in new_summoner]
        #
        #
        # await asyncio.gather(*tasks)
        for summoner in new_summoner:
            self.append(wrap_summoner_obj(summoner))

        print(f'{round(time.time() - s, 4)}  gather')

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

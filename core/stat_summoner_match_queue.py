import time
import traceback

from common.const import Status
from common.db import (
    connect_sql_aurora,
    RDS_INSTANCE_TYPE,
    sql_execute,
    sql_execute_dict,
)
from common.utils import get_changed_current_obj_status, get_current_datetime, logging_time
from core.stat_queue_sys import QueueOperator

from helper.stat_summoner_match import wait_func, work_func

from model.summoner_model import WaitingSummonerMatchObj, WaitingSummonerObj
from typing import Callable, Tuple
from datetime import datetime


def wrap_summoner_obj(obj: Tuple[str, str]) -> WaitingSummonerObj:
    platform_id, puu_id = obj
    return WaitingSummonerObj(
        platform_id=platform_id,
        puu_id=puu_id,
        status=2
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
    def update_new_data(self):
        with connect_sql_aurora(RDS_INSTANCE_TYPE.READ) as conn:
            new_working = set(sql_execute(
                'SELECT distinct platform_id, puu_id '
                'from b2c_summoner_match_queue '
                f'WHERE status={Status.Working.code} '
                f'order by reg_datetime asc ',
                conn)
            )

        exist_working = {tuple(x.__dict__.values()) for x in self.working_status.deque}
        new_working_removed_dupl = list(map(wrap_summoner_obj, new_working.difference(exist_working)))

        s = time.time()
        if len(exist_working) == 0:
            self.working_status.reinit(sorted(new_working_removed_dupl, key=lambda x: x.reg_datetime))
        else:
            self.working_status.extend(new_working_removed_dupl)

        print(f'{get_current_datetime()} | Updated ({time.time() - s} processed)')

    @logging_time
    async def process_job(self, current_obj: WaitingSummonerMatchObj):
        try:
            suitable_func = self.search_suitable_process_func(current_obj)
            func_return = await suitable_func(current_obj)
            changed_current_obj_status_code = get_changed_current_obj_status(current_obj, func_return)

        except Exception:
            changed_current_obj_status_code = Status.Error.code
            if current_obj.status == Status.Waiting.code:
                self.waiting_status.append(current_obj)
            elif current_obj.status == Status.Working.code:
                self.working_status.append(current_obj)
            print(traceback.format_exc())

        finally:
            with connect_sql_aurora(RDS_INSTANCE_TYPE.READ) as conn:
                sql_execute_dict(
                    'UPDATE b2c_summoner_match_queue '
                    f'SET status = {changed_current_obj_status_code} '
                    f'WHERE platform_id = {repr(current_obj.platform_id)} '
                    f'and puu_id = {repr(current_obj.puu_id)} '
                    f'and status = {current_obj.status} '
                    # f'and reg_datetime = "{str(current_obj.reg_datetime)}"'
                    ,conn
                )
                conn.commit()
            self.update_last_obj(current_obj)
            self.update_last_change_status(changed_current_obj_status_code)


    @staticmethod
    def search_suitable_process_func(current_obj: WaitingSummonerMatchObj):
        if current_obj.status == Status.Waiting.code:
            return wait_func
        elif current_obj.status == Status.Working.code:
            return work_func

    def print_remain(self):
        with connect_sql_aurora(RDS_INSTANCE_TYPE.READ) as conn:
            count = sql_execute(
                f'SELECT count(*) '
                f'FROM b2c_summoner_match_queue '
                f'WHERE status = {Status.Working.code}'
                , conn
            )
        print(f'\n - Remain\n'
              f'\tMatch Waiting: {count[0][0]} ')

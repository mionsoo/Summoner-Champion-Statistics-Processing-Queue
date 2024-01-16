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

from helper.stat_summoner_match import wait_func, work_func

from model.summoner_model import WaitingSummonerMatchObj
from typing import Callable


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
            new_waiting = set(sql_execute(
                'SELECT platform_id, puu_id, status, reg_datetime, match_id '
                'from b2c_summoner_match_queue '
                f'WHERE status={Status.Waiting.code} '
                f'order by reg_datetime desc',
                conn)
            )
            new_working = set(sql_execute(
                'SELECT platform_id, puu_id, status, reg_datetime, match_id '
                'from b2c_summoner_match_queue '
                f'WHERE status={Status.Working.code} '
                f'order by reg_datetime desc',
                conn)
            )

        exist_waiting = {tuple(x.__dict__.values()) for x in self.waiting_status.deque}
        new_waiting_removed_dupl = list(map(wrap_summoner_match_obj, new_waiting.difference(exist_waiting)))

        exist_working = {tuple(x.__dict__.values()) for x in self.working_status.deque}
        new_working_removed_dupl = list(map(wrap_summoner_match_obj, new_working.difference(exist_working)))

        if len(exist_waiting) == 0:
            self.waiting_status.reinit(new_waiting_removed_dupl)
        else:
            self.waiting_status.extend_left(new_waiting_removed_dupl)

        if len(exist_working) == 0:
            self.working_status.reinit(new_working_removed_dupl)
        else:
            self.working_status.extend_left(new_waiting_removed_dupl)



    def process_job(self, current_obj: WaitingSummonerMatchObj):
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
                    'UPDATE b2c_summoner_match_queue '
                    f'SET status = {changed_current_obj_status_code} '
                    f'WHERE platform_id = {repr(current_obj.platform_id)} '
                    f'and puu_id = {repr(current_obj.puu_id)} '
                    f'and status = {current_obj.status} '
                    f'and reg_datetime = "{str(current_obj.reg_datetime)}"',
                    conn
                )
                conn.commit()
            self.update_last_obj(current_obj)
            self.update_last_change_status(changed_current_obj_status_code)
        time.sleep(10)

    @staticmethod
    def search_suitable_process_func(current_obj: WaitingSummonerMatchObj)-> Callable:
        if current_obj.status == Status.Waiting.code:
            return wait_func
        elif current_obj.status == Status.Working.code:
            return work_func


import traceback

from common.const import Status
from common.db import connect_sql_aurora, RDS_INSTANCE_TYPE, sql_execute_dict
from core.stat_queue_sys import QueueStatus
from helper.stat_summoner import wait_func, work_func, change_current_obj_status
from model.summoner_model import WaitingSummonerObj
from typing import Callable


class SummonerQueueOperator:
    def __init__(self):
        self.waiting_status = QueueStatus(criterion=Status.Waiting.code)
        self.working_status = QueueStatus(criterion=Status.Working.code)

    def append(self, obj: WaitingSummonerObj):
        if obj.status == Status.Waiting.code:
            self.waiting_status.append_left(obj)
        elif obj.status == Status.Working.code:
            self.working_status.append_left(obj)

    def update_new_data(self):
        with connect_sql_aurora(RDS_INSTANCE_TYPE.READ) as conn:
            waiting = sql_execute_dict(
                'SELECT platform_id, puu_id, status, reg_datetime '
                'from b2c_summoner_queue '
                f'WHERE status={Status.Waiting.code} '
                f'or status={Status.Working.code}',
                conn
            )

        for i in waiting:
            obj_insert = WaitingSummonerObj(**i)
            self.append(obj_insert)

    def get_current_obj(self) -> WaitingSummonerObj | None:
        if self.waiting_status.count >= 1:
            return self.waiting_status.pop()

        elif self.working_status.count >= 1:
            return self.working_status.pop()

        else:
            return None

    def is_all_queue_is_empty(self) -> bool:
        return self.working_status.count == 0 and self.waiting_status.count == 0

    def is_data_exists(self) -> bool:
        return self.working_status.count != 0 or self.waiting_status.count != 0

    def process_job(self, current_obj):
        try:
            suitable_func = self.search_suitable_process_func(current_obj)
            func_return = suitable_func(current_obj)
            changed_current_obj_status = change_current_obj_status(current_obj, func_return)

        except Exception:
            changed_current_obj_status = Status.Error.code
            self.append(current_obj)
            print(traceback.format_exc())

        finally:
            with connect_sql_aurora(RDS_INSTANCE_TYPE.READ) as conn:
                sql_execute_dict(
                    'UPDATE b2c_summoner_queue '
                    f'SET status = {changed_current_obj_status} '
                    f'WHERE platform_id = {repr(current_obj.platform_id)} '
                    f'and puu_id = {repr(current_obj.puu_id)} '
                    f'and status = {current_obj.status} '
                    f'and reg_datetime = "{str(current_obj.reg_datetime)}"',
                    conn
                )
                conn.commit()

    @staticmethod
    def search_suitable_process_func(current_obj: WaitingSummonerObj) -> Callable:
        if current_obj.status == Status.Waiting.code:
            return wait_func
        elif current_obj.status == Status.Working.code:
            return work_func

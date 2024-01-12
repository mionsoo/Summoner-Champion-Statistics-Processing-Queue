import traceback
from core.stat_queue_sys import QueueOperator
from model.summoner_model import WaitingSummonerObj, WaitingSummonerMatchObj
from helper.stat_summoner_match import wait_func, work_func
from common.db import connect_sql_aurora, RDS_INSTANCE_TYPE, sql_execute_dict
from common.const import Status
from common.utils import change_current_obj_status


class SummonerMatchQueueOperator(QueueOperator):
    def update_new_data(self):
        with connect_sql_aurora(RDS_INSTANCE_TYPE.READ) as conn:
            waiting = sql_execute_dict(
                'SELECT platform_id, puu_id, status, reg_datetime, match_id '
                'from b2c_summoner_match_queue '
                f'WHERE status={Status.Waiting.code} '
                f'or status={Status.Working.code}',
                conn
            )

        for i in waiting:
            obj_insert = WaitingSummonerMatchObj(**i)
            self.append(obj_insert)

    def process_job(self, current_obj: WaitingSummonerMatchObj):
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
                    'UPDATE b2c_summoner_match_queue '
                    f'SET status = {changed_current_obj_status} '
                    f'WHERE platform_id = {repr(current_obj.platform_id)} '
                    f'and puu_id = {repr(current_obj.puu_id)} '
                    f'and status = {current_obj.status} '
                    f'and reg_datetime = "{str(current_obj.reg_datetime)}"',
                    conn
                )
                conn.commit()

    @staticmethod
    def search_suitable_process_func(current_obj: WaitingSummonerMatchObj):
        if current_obj.status == Status.Waiting.code:
            return wait_func
        elif current_obj.status == Status.Working.code:
            return work_func


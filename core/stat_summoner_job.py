from core.stat_job import Job, JobResult
from common.const import Status
from common.utils import get_changed_current_obj_status
from model.summoner_model import WaitingSummonerObj

import traceback

from helper.stat_summoner import wait_func, work_func


class StatQueueSummonerJob(Job):
    @staticmethod
    def search_suitable_process_func(current_obj: WaitingSummonerObj):
        if current_obj.status == Status.Waiting.code:
            return wait_func
        elif current_obj.status == Status.Working.code:
            return work_func

    async def process(self, match_ids) -> JobResult:
        result_status = Status.Error.code
        func_return = None

        try:
            suitable_func = self.search_suitable_process_func(self.obj)
            func_return = await suitable_func(self.obj, match_ids)
            result_status = await get_changed_current_obj_status(self.obj, func_return)

        except Exception:
            print(traceback.format_exc())

        finally:
            return JobResult(
                data=func_return,
                target_obj=self.obj,
                result_status=result_status
            )
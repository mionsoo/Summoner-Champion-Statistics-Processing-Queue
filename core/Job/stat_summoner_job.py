import traceback

from common.const import JobStatus
from common.utils import get_changed_current_obj_status
from core.Job.stat_job import Job, JobResult
from helper.Summoner import wait_func, work_func
from model.Summoner import WaitingSummonerJob


class StatQueueSummonerJob(Job):
    @staticmethod
    def search_suitable_process_func(current_obj: WaitingSummonerJob):
        if current_obj.status == JobStatus.Waiting.type:
            return wait_func
        elif current_obj.status == JobStatus.Working.type:
            return work_func

    async def process(self, match_ids=None) -> JobResult:
        result_status = JobStatus.Error.type
        func_return = None

        try:
            suitable_func = self.search_suitable_process_func(self.obj)
            func_return = await suitable_func(self.obj)
            result_status = await get_changed_current_obj_status(self.obj, func_return)

        except Exception:
            print(traceback.format_exc())

        return JobResult(data=func_return, target_obj=self.obj, result_status=result_status)

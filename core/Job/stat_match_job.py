import traceback

from common.const import JobStatus
from common.utils import get_changed_current_obj_status
from core.Job.stat_job import Job, JobResult
from helper.Match import wait_func, work_func
from model.Summoner import WaitingSummonerMatchJob


class StatQueueMatchJob(Job):
    @staticmethod
    def search_suitable_process_func(current_obj: WaitingSummonerMatchJob):
        if current_obj.status == JobStatus.Waiting.type:
            return wait_func
        elif current_obj.status == JobStatus.Working.type:
            return work_func

    async def process(self, match_ids=None) -> JobResult:
        result_status = JobStatus.Error.type
        queries = None

        try:
            suitable_func = self.search_suitable_process_func(self.obj)
            queries, func_return = await suitable_func(self.obj, match_ids)
            result_status = await get_changed_current_obj_status(self.obj, func_return)

        except Exception:
            print(traceback.format_exc())

        return JobResult(data=queries, target_obj=self.obj, result_status=result_status)

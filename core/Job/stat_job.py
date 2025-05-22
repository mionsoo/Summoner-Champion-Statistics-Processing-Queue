from abc import *

from model.Summoner import WaitingSummonerMatchJob, WaitingSummonerJob


class JobResult:
    def __init__(self, data, target_obj: WaitingSummonerJob | WaitingSummonerMatchJob, result_status):
        self.data = data
        self.target_job = target_obj
        self.processed_status = result_status


class Job(metaclass=ABCMeta):
    def __init__(self, current_obj):
        self.obj = current_obj

    @abstractmethod
    async def process(self, match_ids=None):
        pass

    @staticmethod
    @abstractmethod
    def search_suitable_process_func(current_obj: WaitingSummonerJob | WaitingSummonerMatchJob):
        pass

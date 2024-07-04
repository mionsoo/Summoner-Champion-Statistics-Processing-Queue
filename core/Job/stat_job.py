from abc import *
from model.summoner_model import WaitingSummonerMatchObj, WaitingSummonerObj


class JobResult:
    def __init__(self, data, target_obj, result_status):
        self.data = data
        self.target_obj = target_obj
        self.result_status = result_status

class Job(metaclass=ABCMeta):
    def __init__(self, current_obj):
        self.obj = current_obj

    @abstractmethod
    async def process(self, match_ids=None):
        pass

    @staticmethod
    @abstractmethod
    def search_suitable_process_func(current_obj: WaitingSummonerObj | WaitingSummonerMatchObj):
        pass



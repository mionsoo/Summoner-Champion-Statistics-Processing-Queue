import datetime

from pydantic import BaseModel


class WaitingSummonerJob(BaseModel):
    platform_id: str = ""
    puu_id: str = ""
    reg_date: datetime.date = datetime.date(2000, 1, 1)
    status: int = -1
    reg_datetime: datetime.datetime = datetime.datetime(2000, 1, 1, 0)
    season: int = 0
    season_start_timestamp: datetime.datetime = datetime.datetime(2000, 1, 1, 0)
    season_end_timestamp: datetime.datetime = datetime.datetime(2000, 1, 1, 0)


class WaitingSummonerMatchJob(WaitingSummonerJob):
    match_id: str = ""

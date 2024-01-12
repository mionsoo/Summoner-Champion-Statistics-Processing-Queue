from pydantic import BaseModel
import datetime


class WaitingSummonerObj(BaseModel):
    platform_id: str = ''
    puu_id: str = ''
    status: int = -1
    reg_datetime: datetime.datetime = datetime.datetime(2000, 1, 1, 0)



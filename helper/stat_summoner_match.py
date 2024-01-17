import json
import requests

from common.utils import get_current_datetime
from model.summoner_model import WaitingSummonerMatchObj


def wait_func(current_obj: WaitingSummonerMatchObj) -> None:
    return None


def work_func(current_obj: WaitingSummonerMatchObj) -> int | None:
    req_data = {
        "platform_id": current_obj.platform_id,
        "puu_id": current_obj.puu_id,
        "match_id": current_obj.match_id
    }
    req_headers = {
        "Referer": 'deeplol.gg'
    }
    url = 'https://renew.deeplol.gg/match/stats-async'
    with requests.post(url, data=json.dumps(req_data), headers=req_headers) as req:
        r = req.json()

    print(f'{get_current_datetime()} | ',current_obj.match_id, current_obj.platform_id, current_obj.puu_id)
    print(f'{get_current_datetime()} | ', r['msg'])

    if r['msg'].split(', ')[-1] == 'insert success':
        return None

    return 1

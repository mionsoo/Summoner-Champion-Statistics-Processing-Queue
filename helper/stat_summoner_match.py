from model.summoner_model import WaitingSummonerMatchObj
from common.db import connect_sql_aurora, RDS_INSTANCE_TYPE, sql_execute, sql_execute_dict
import requests
import json


def wait_func(current_obj: WaitingSummonerMatchObj):
    return None


def work_func(current_obj: WaitingSummonerMatchObj):
    req_data = {
        "platform_id": current_obj.platform_id,
        "puu_id": current_obj.puu_id,
        "match_id": current_obj.match_id
    }
    req_headers = {
        "Referer": 'deeplol.gg'
    }
    url = 'https://renew.deeplol.gg/match/stats-async'
    print(current_obj.match_id, current_obj.platform_id, current_obj.puu_id)
    with requests.post(url, data=json.dumps(req_data), headers=req_headers) as req:
        r = req.json()
    print(r['msg'])

    if r['msg'].split(', ')[-1] == 'insert success':
        return None

    return 1

import json
import requests
import aiohttp
import asyncio

from common.utils import get_current_datetime
from common.const import Status
from model.summoner_model import WaitingSummonerMatchObj, WaitingSummonerObj
from common.db import connect_sql_aurora, sql_execute, RDS_INSTANCE_TYPE


async def wait_func(current_obj: WaitingSummonerMatchObj) -> None:
    await asyncio.sleep(0)
    return None


async def work_func(current_obj: WaitingSummonerObj) -> int | None:
    print(f'{get_current_datetime()} | ', *current_obj.__dict__.values())
    with connect_sql_aurora(RDS_INSTANCE_TYPE.READ) as conn:    
        match_ids = sum(list(
            sql_execute(
                'SELECT match_id '
                'FROM b2c_summoner_match_queue '
                f'WHERE puu_id = {repr(current_obj.puu_id)} '
                f'and platform_id = {repr(current_obj.platform_id)} '
                f'and status = {Status.Working.code}'
                , conn
            )
        ), ())
    print(f'{get_current_datetime()} | Num of requests: {len(match_ids)}\n(', *match_ids,')')


    async with aiohttp.ClientSession() as client:
        tasks = [asyncio.create_task(request_stats_async(current_obj, match_id, client)) for match_id in match_ids]
        results = await asyncio.gather(*tasks)

    if sum(map(lambda x: x.split(', ')[-1] == 'insert success', results)) == len(match_ids):
        return None

    return 1


async def request_stats_async(current_obj, match_id, client):
    print(match_id)
    req_data = {
        "platform_id": current_obj.platform_id,
        "puu_id": current_obj.puu_id,
        "match_id": match_id,
        'batch': 1
    }
    req_headers = {
        "Referer": 'deeplol.gg'
    }
    url = 'https://renew.deeplol.gg/match/stats-async'
    async with client.post(url, data=json.dumps(req_data), headers=req_headers) as response:
        r = await response.json()
        return r['msg']


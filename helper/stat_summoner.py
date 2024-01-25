import aiohttp

import asyncio
import json

from common.db import (
    sql_execute,
    connect_sql_aurora,
    riot_api_key,
    RDS_INSTANCE_TYPE
)

from common.const import Status
from model.summoner_model import WaitingSummonerObj


async def request_stats_async(current_obj, client):
    req_headers = {
        "Referer": 'deeplol.gg'
    }
    url = (
        f'https://renew.deeplol.gg/batch/stat_queue_summoner?'
        f'platform_id={current_obj.platform_id}'
        f'&puu_id={current_obj.puu_id}'
    )
    async with client.get(url, headers=req_headers) as response:
        r = await response.json()
        return r


async def wait_func(current_obj: WaitingSummonerObj) -> int | None:
    async with aiohttp.ClientSession() as client:
        result = await request_stats_async(current_obj, client)

    if result['msg'] == 'no match':
        return None

    return 1






def work_func(current_obj) -> int | None:
    with connect_sql_aurora(RDS_INSTANCE_TYPE.READ) as conn:
        not_finished_jobs = sql_execute(
            'SELECT match_id, status '
            'FROM b2c_summoner_match_queue '
            f'WHERE platform_id={repr(current_obj.platform_id)} '
            f'and puu_id={repr(current_obj.puu_id)} '
            f'and (status != {Status.Success.code} '
            f'and status != {Status.Error.code})',
            conn
        )

    if len(not_finished_jobs) >= 1:
        return 1

    return None



import asyncio

import aiohttp
import json

from common.db import connect_sql_aurora_async, RDS_INSTANCE_TYPE
from common.const import Status
from common.utils import get_current_datetime
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
        data = await response.read()
        r = json.loads(data)
        return r


async def wait_func(current_obj: WaitingSummonerObj, conn=None) -> int | None:
    async with aiohttp.ClientSession() as client:
        result = await request_stats_async(current_obj, client)

    if result['msg'] == 'no match':
        return None

    return 1


async def work_func(current_obj, conn):
    async with aiohttp.ClientSession() as client:
        result = await request_stats_async_work(current_obj, client)

    if len(result['msg']) > 1:
        return 1

    return None
    #
    # _conn = await connect_sql_aurora_async(RDS_INSTANCE_TYPE.READ)
    # async with conn.cursor() as cursor:
    #     await cursor.execute(
    #         'SELECT match_id, status '
    #         'FROM b2c_summoner_match_queue '
    #         f'WHERE platform_id={repr(current_obj.platform_id)} '
    #         f'and puu_id={repr(current_obj.puu_id)} '
    #         f'and (status != {Status.Success.code} '
    #         f'and status != {Status.Error.code})'
    #     )
    #
    # not_finished_jobs = await cursor.fetchall()
    #
    #
    # if len(not_finished_jobs) >= 1:
    #     return 1
    #
    # return None


async def get_season(client):
    req_headers = {
        "Referer": 'deeplol.gg'
    }
    url = 'https://renew.deeplol.gg/common/season-list'
    async with client.get(url, headers=req_headers) as response:
        data = await response.read()
        r = json.loads(data)
        return r['season_list'][0]


async def update_summoner_stat_dynamo(current_obj):
    try:
        async with aiohttp.ClientSession() as client:
            season = await get_season(client)
    except Exception:
        season = 19

    async with aiohttp.ClientSession() as client:
        t1 = asyncio.create_task(request_stats(current_obj, 'RANKED', season, client))
        t2 = asyncio.create_task(request_stats(current_obj, 'ARAM', season, client))
        t3 = asyncio.create_task(request_stats(current_obj, 'URF', season, client))

        await asyncio.gather(t1, t2, t3)
    print(f'{get_current_datetime()} | request stats success')


async def request_stats(current_obj, queue_type, season, client):
    req_data = {
        "platform_id": current_obj.platform_id,
        "puu_id": current_obj.puu_id,
        "season": season,
        "queue_type": queue_type
    }
    req_headers = {
        "Referer": 'deeplol.gg',
        'Content-Type': 'application/json'
    }
    url = 'https://renew.deeplol.gg/match/stats'
    async with client.post(url, data=json.dumps(req_data), headers=req_headers) as response:
        data = await response.read()
        r = json.loads(data)

        if r['msg'] != 'insert success':
            print('stats: ', r)
        return r['msg']


async def request_stats_async_work(current_obj, client):
    req_headers = {
        "Referer": 'deeplol.gg'
    }
    url = (
        f'https://renew.deeplol.gg/batch/stat_queue_summoner_work?'
        f'platform_id={current_obj.platform_id}'
        f'&puu_id={current_obj.puu_id}'
    )
    async with client.get(url, headers=req_headers) as response:
        data = await response.read()
        r = json.loads(data)
        return r
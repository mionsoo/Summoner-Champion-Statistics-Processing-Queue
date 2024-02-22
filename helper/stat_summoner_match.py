import json
import aiohttp
import asyncio

from common.utils import get_current_datetime
from model.summoner_model import WaitingSummonerMatchObj, WaitingSummonerObj
from common.const import Status


async def wait_func(current_obj: WaitingSummonerMatchObj, match_ids) -> None:
    await asyncio.sleep(0)
    return None


async def make_queries(current_obj, results):
    await asyncio.sleep(0)
    # q = [(x.split(', ')[0], current_obj.puu_id, current_obj.platform_id, str(current_obj.reg_date),
    #       Status.Success.code if x.split(', ')[1] == 'insert success' else Status.Error.code) async for x in results]

    q = []
    for x in results:
        match_id = x.split(', ')[0]
        puu_id = current_obj.puu_id
        platform_id = current_obj.platform_id
        # reg_date = str(current_obj.reg_date)
        status = Status.Error.code if x.endswith('서버측 에러로 매치 업데이트에 실패했습니다.') or x.split(', ')[1] == 'error' else Status.Success.code
        q.append((match_id, puu_id, platform_id, status))
    return q

async def work_func(current_obj: WaitingSummonerObj, match_ids):
    print(f'{get_current_datetime()} | ', *current_obj.__dict__.values())
    print(f'{get_current_datetime()} | Num of requests: {len(match_ids)}')

    try:
        async with aiohttp.ClientSession() as client:
            tasks = [asyncio.create_task(request_stats_async(current_obj, match_id, client)) for match_id in match_ids]
            results = await asyncio.gather(*tasks)
    except:
        pass
    finally:
        try:
            q = await make_queries(current_obj, results)
        except:
            print(f'results: {results} ')

    # if sum(map(lambda x: x.split(', ')[-1] == 'insert success', results)) == len(match_ids):
    #     return None

    return q, None


async def request_stats_async(current_obj, match_id, client):
    req_data = {
        "platform_id": current_obj.platform_id,
        "puu_id": current_obj.puu_id,
        "match_id": match_id,
        'batch': 1
    }
    req_headers = {
        "Referer": 'deeplol.gg',
        'Content-Type': 'application/json'
    }
    url = 'https://renew.deeplol.gg/match/stats-async'
    try:
        async with client.post(url, data=json.dumps(req_data), headers=req_headers) as response:
            z = await response.text()
            print(z)
            r = await response.json()
            return r['msg']
    except:
        print(f'{match_id}, error')
        return f'{match_id}, error'

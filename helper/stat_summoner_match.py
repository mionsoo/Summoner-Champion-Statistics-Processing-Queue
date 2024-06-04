import json
import aiohttp
import asyncio

from common.utils import get_current_datetime
from model.summoner_model import WaitingSummonerMatchObj, WaitingSummonerObj
from model.match_model import BatchStatQueueContainer
from common.const import Status


async def wait_func(current_obj: WaitingSummonerMatchObj, match_ids) -> None:
    await asyncio.sleep(0)
    return None


async def make_queries(current_obj, results):
    await asyncio.sleep(0)

    q = []
    for x in results:
        match_id, result_msg = x.split(', ')
        puu_id = current_obj.puu_id
        platform_id = current_obj.platform_id
        status = Status.Success.code
        if (
                result_msg == 'error'
                or x.endswith('해당 (유저의) 경기가 존재하지 않습니다.')
                or x.endswith('서버측 에러로 매치 업데이트에 실패했습니다.')
        ):
            status = Status.Error.code

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


    # if sum(map(lambda x: x.split(', ')[-1] == 'insert success', results)) == len(match_ids):
    #     return None


    return results, None


async def request_stats_async(current_obj, match_id, client):
    # req_data = {
    #     "platform_id": current_obj.platform_id,
    #     "puu_id": current_obj.puu_id,
    #     "match_id": match_id,
    #     'batch': 1
    # }
    req_data = {
            "platform_id": current_obj.platform_id,
            "match_id": match_id,
        }
    req_headers = {
        "Referer": 'deeplol.gg',
        'Content-Type': 'application/json'
    }
    url = f'https://renew.deeplol.gg/batch/stat-async?platform_id={current_obj.platform_id}&match_id={match_id}'
    try:
        async with client.get(url, data=json.dumps(req_data), headers=req_headers) as response:
            data = await response.read()
            r = json.loads(data)
        return [BatchStatQueueContainer(**i)for i in r['msg'].values()]
    except Exception as e:
        print(f'{current_obj.platform_id} {match_id}, error: {data.decode("utf-8")}')
        return [f'{match_id}, {current_obj.platform_id}, {current_obj.puu_id}, error']

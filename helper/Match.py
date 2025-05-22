import asyncio
import json

import aiohttp

from common.const import JobStatus
from common.utils import get_current_datetime
from model.Match import BatchStatQueueContainer
from model.Summoner import WaitingSummonerMatchJob, WaitingSummonerJob


async def wait_func(current_obj: WaitingSummonerMatchJob, match_ids) -> None:
    await asyncio.sleep(0)
    return None


async def request_stats_async(current_obj, match_id, client):
    req_data = {"platform_id": current_obj.platform_id, "match_id": match_id}
    req_headers = {"Referer": "deeplol.gg", "Content-Type": "application/json"}
    url = f"https://renew.deeplol.gg/batch/stat-async?platform_id={current_obj.platform_id}&match_id={match_id}"

    try:
        async with client.get(url, data=json.dumps(req_data), headers=req_headers) as response:
            data = await response.read()
            r = json.loads(data)

        return [BatchStatQueueContainer(**i) for i in r["msg"].values()]

    except Exception:
        error_msg = data.decode("utf-8")
        sys_status_code = JobStatus.Error.type
        if "Gateway" in error_msg:
            sys_status_code = JobStatus.Timeout.type

        print(f'{current_obj.platform_id} {match_id}, error: {data.decode("utf-8")}')
        return [f"{match_id}, {current_obj.platform_id}, {current_obj.puu_id}, {sys_status_code}, error"]


async def work_func(current_obj: WaitingSummonerJob, match_ids):
    print(f"{get_current_datetime()} | ", *current_obj.__dict__.values())
    print(f"{get_current_datetime()} | Num of requests: {len(match_ids)}")

    try:
        async with aiohttp.ClientSession() as client:
            tasks = [asyncio.create_task(request_stats_async(current_obj, match_id, client)) for match_id in match_ids]
            results = await asyncio.gather(*tasks)
    except:
        pass

    return results, None

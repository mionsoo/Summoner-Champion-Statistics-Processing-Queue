import json

import aiohttp

from model.summoner_model import WaitingSummonerObj


async def request_stats_async_wait(current_obj, client):
    req_headers = {"Referer": "deeplol.gg"}
    url = (
        f"https://renew.deeplol.gg/batch/stat_queue_summoner?"
        f"platform_id={current_obj.platform_id}"
        f"&puu_id={current_obj.puu_id}"
        f"&season_start_timestamp={int(current_obj.season_start_timestamp.timestamp())}"
        f"&season_end_timestamp={int(current_obj.season_end_timestamp.timestamp())}"
    )

    async with client.get(url, headers=req_headers) as response:
        data = await response.read()
        try:
            r = json.loads(data)
        except:
            return None
        else:
            return r


async def wait_func(current_obj: WaitingSummonerObj, conn=None) -> set | None:
    async with aiohttp.ClientSession() as client:
        result = await request_stats_async_wait(current_obj, client)

    if result is None or result["msg"] == "no match" or "error" in result["msg"]:
        return None

    api_called_match_ids_stats = set(result["msg"].split(", "))

    return api_called_match_ids_stats


async def request_stats_async_work(current_obj, client):
    req_headers = {"Referer": "deeplol.gg"}
    url = (
        f"https://renew.deeplol.gg/batch/stat_queue_summoner_work?"
        f"platform_id={current_obj.platform_id}"
        f"&puu_id={current_obj.puu_id}"
    )
    try:
        async with client.get(url, headers=req_headers) as response:
            data = await response.read()
            r = json.loads(data)
            return r
    except json.decoder.JSONDecodeError:
        return None


async def work_func(current_obj):
    async with aiohttp.ClientSession() as client:
        result = await request_stats_async_work(current_obj, client)

    if result is None:
        return None

    if len(result["msg"]) > 1:
        return -1

    return None
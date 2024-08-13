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
    async with _conn.cursor() as cursor:
        await cursor.execute(
            "SELECT match_id, status "
            "FROM b2c_summoner_match_queue "
            f"WHERE platform_id={repr(platform_id)} "
            f"and puu_id={repr(puu_id)} "
            f"and (status != {Status.Success.code} "
            f"and status != {Status.Error.code} "
            f"and status != {Status.Timeout.code})"
        )
    async with aiohttp.ClientSession() as client:
        result = await request_stats_async_work(current_obj, client)

    if result is None:
        return None

    if len(result["msg"]) > 1:
        return -1

    return None
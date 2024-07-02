from common.const import Status


async def execute_matches(current_obj, cursor):
    await cursor.execute(
        'SELECT match_id '
        'FROM b2c_summoner_match_queue '
        f'WHERE puu_id = {repr(current_obj.puu_id)} '
        f'and platform_id = {repr(current_obj.platform_id)} '
        f'and status = {Status.Working.code} '
    )
    result = await cursor.fetchall()
    match_ids = sum(list(result), ())

    return match_ids

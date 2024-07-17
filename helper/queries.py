from common.const import Status
from common.db import make_summoner_insert_query


async def execute_matches(current_obj, cursor):
    await cursor.execute(
        "SELECT match_id "
        "FROM b2c_summoner_match_queue "
        f"WHERE puu_id = {repr(current_obj.puu_id)} "
        f"and platform_id = {repr(current_obj.platform_id)} "
        f"and status = {Status.Working.code} "
    )
    result = await cursor.fetchall()
    match_ids = sum(list(result), ())

    return match_ids


async def insert_summoner_champion_stats(conn, cursor, table_alias, value_query, duplicate_key_update_query):
    await cursor.execute(
        query=(
            f"INSERT INTO b2c_summoner_champion_stats_partitioned(puu_id, match_id, platform_id, season, creation_timestamp ,queue_id, position, champion_id,"
            f"enemy_champion_id, is_win,is_remake ,is_runaway ,kills, deaths, assists, damage_taken, damage_dealt, cs, gold_diff_15, gold_per_team,"
            f"damage_per_team, game_duration, gold, kill_point, vision_score, penta_kills, quadra_kills, triple_kills, "
            f"double_kills,top, jungle, middle, bot, supporter,cs_15) "
            f"values {value_query} as {table_alias} "
            f"ON DUPLICATE KEY UPDATE"
            f"{duplicate_key_update_query}"
        )
    )
    await conn.commit()


async def execute_update_queries_summoner(conn, job_results):
    async with conn.cursor() as cursor:
        bulk_items = ", ".join([make_summoner_insert_query(result) for result in job_results])

        await cursor.execute(
            "INSERT INTO b2c_summoner_queue(puu_id, platform_id, status, reg_date, reg_datetime) "
            f"VALUES{bulk_items} as queue "
            f"ON DUPLICATE KEY UPDATE status=queue.status"
        )
        await conn.commit()
        return 0


async def execute_summoner_insert_query(conn, cursor, queries):
    await cursor.execute(
        "INSERT INTO b2c_summoner_queue(puu_id, platform_id, status, reg_date, reg_datetime) "
        f"VALUES{queries} as queue "
        f"ON DUPLICATE KEY UPDATE status=queue.status"
    )

    await conn.commit()


async def execute_summoner_match_insert_query(bulk_item, conn, cursor):
    await cursor.execute(
        "INSERT ignore INTO b2c_summoner_match_queue(platform_id, puu_id, match_id, status) " 
        f"VALUES {bulk_item}"
    )
    await conn.commit()


async def update_summoner_matches(conn, cursor, match_id_lists_query):
    await cursor.execute(
        f"INSERT INTO b2c_summoner_match_queue(platform_id, puu_id, match_id, status, reg_date, reg_datetime) "
        f"values {match_id_lists_query} as t "
        f"ON DUPLICATE KEY UPDATE status = t.status"
    )
    await conn.commit()


async def execute_select_inserted_match_id(current_obj, cursor):
    await cursor.execute(
        "SELECT match_id "
        "FROM b2c_summoner_champion_stats_partitioned "
        f"WHERE puu_id = {repr(current_obj.puu_id)} "
        f"and platform_id = {repr(current_obj.platform_id)} "
        f"and season = {current_obj.season} "
        f"and queue_id in (420, 440, 450, 900, 1900)"
    )
    result = await cursor.fetchall()
    return result


async def execute_select_match_obj(cursor, status):
    await cursor.execute(
        "SELECT distinct platform_id, puu_id, reg_date "
        "FROM b2c_summoner_match_queue "
        f"WHERE status={status} "
    )
    result = await cursor.fetchall()
    return result


async def execute_select_match_count(cursor):
    await cursor.execute(
        f"SELECT count(*) "
        f"FROM b2c_summoner_match_queue "
        f"WHERE status = {Status.Working.code}"
    )
    count = await cursor.fetchone()
    return count

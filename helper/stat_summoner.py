from common.db import (
    sql_execute,
    connect_sql_aurora,
    riot_api_key,
    RDS_INSTANCE_TYPE
)
from common.riot import get_json_time_limit, RiotV5Match
from common.const import Status
from model.summoner_model import WaitingSummonerObj

from typing import Tuple
import datetime
import pytz


def get_season_info():
    now = datetime.datetime.utcnow()
    KST = pytz.timezone('Asia/Seoul').localize(now).tzinfo
    current_datetime_timestamp = datetime.datetime.now(KST).timestamp()

    try:
        with connect_sql_aurora(RDS_INSTANCE_TYPE.READ) as conn:
            season, season_start_timestamp, season_end_timestamp = sql_execute(
                "SELECT season, start_timestamp, end_timestamp "
                "FROM b2c_season_info_datetime "
                f"WHERE {int(current_datetime_timestamp)} between start_timestamp and end_timestamp "
                "ORDER BY start_datetime DESC ",
                conn
            )[0]
    except IndexError:
        season = 19
        season_start_timestamp = 1704855600
        season_end_timestamp = 1717167600

    return season, season_start_timestamp, season_end_timestamp


def get_summoner_season_match_ids(current_obj: WaitingSummonerObj, season_timestamp: Tuple[int, int], queue_type: str):
    match_v5 = RiotV5Match(
        api_key=riot_api_key,
        platform_id=current_obj.platform_id,
        puu_id=current_obj.puu_id
    )

    start_idx = 0
    season_start_timestamp, season_end_timestamp = season_timestamp
    api_called_match_ids = []

    while True:
        url = match_v5.get_match_ids_url(
            queue_type=queue_type,
            start_idx =start_idx,
            count=100,
            start_time=season_start_timestamp,
            end_time=season_end_timestamp
        )
        match_ids = get_json_time_limit(url, time_limit=5).json()
        start_idx += 100

        if not match_ids:
            break
        api_called_match_ids += match_ids

    return api_called_match_ids


def make_bulk_value_string_insert_summoner_match_queue(current_obj, match_id):
    return f'({repr(current_obj.platform_id)}, {repr(current_obj.puu_id)}, {repr(match_id)}, {Status.Working.code})'


def wait_func(current_obj: WaitingSummonerObj):
    season, *season_timestamp = get_season_info()
    api_called_match_ids_ranked = set(get_summoner_season_match_ids(current_obj, season_timestamp, 'RANKED'))
    api_called_match_ids_urf = set(get_summoner_season_match_ids(current_obj, season_timestamp, 'PICK_URF'))
    api_called_match_ids_aram = set(get_summoner_season_match_ids(current_obj, season_timestamp, 'ARAM'))

    api_called_match_ids_stats = api_called_match_ids_ranked | api_called_match_ids_urf | api_called_match_ids_aram

    with connect_sql_aurora(RDS_INSTANCE_TYPE.READ) as conn:
        db_called_match_ids = sql_execute(
            query='SELECT match_id '
                  'FROM b2c_summoner_champion_stats_partitioned '
                  f'WHERE puu_id = {repr(current_obj.puu_id)} '
                  f'and platform_id = {repr(current_obj.platform_id)} '
                  f'and season = {season}',
            conn=conn
        )

    db_called_match_ids = set(sum(db_called_match_ids, ()))
    remove_duplicated_match_ids = api_called_match_ids_stats.difference(db_called_match_ids)

    if not remove_duplicated_match_ids:
        return None

    bulk_item = ', '.join([make_bulk_value_string_insert_summoner_match_queue(current_obj, match_id) for match_id in remove_duplicated_match_ids])
    with connect_sql_aurora(RDS_INSTANCE_TYPE.READ) as conn:
        sql_execute(
            'INSERT INTO b2c_summoner_match_queue '
            '(platform_id, puu_id, match_id, status) '
            f'VALUES {bulk_item}', conn
        )
        conn.commit()

    return 1


def work_func(current_obj):
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



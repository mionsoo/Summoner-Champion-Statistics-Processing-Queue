import sys
sys.path.append("/usr/src/app")
import copy
import os
import time
import traceback
from pydantic import BaseModel
from datetime import datetime, timedelta
from common.db import sql_execute, connect_sql_aurora, conf_dict, riot_api_key, RDS_INSTANCE_TYPE
import redis
from common.riot import get_json_time_limit, RiotV4Tier, RiotV4Summoner, RiotV1Accounts, RiotV1Challenges
from enum import Enum, auto
from dataclasses import dataclass
from pytz import timezone
import requests

host = 'redis_queue' if os.environ["API_ENV"] == "dev" else os.environ['HOST']
rd = redis.Redis(host=host, port=6379, decode_responses=True)

SERVER_NOT_WORKING = ['TW2']


class StrEnum(str, Enum):
    def _generate_next_value_(name, start, count, last_values):
        return name

    def __str__(self):
        return self.name


class API_TYPE(StrEnum):
    summoner = auto()
    match = auto()
    league = auto()


@dataclass
class ApiInfo:
    riot_id_name: str = ''
    riot_id_tag_line: str = ''
    summoner_id: str = ''
    account_id: str = ''
    puu_id: str = ''
    match_id: str = ''
    platform_id: str = ''
    api_type: str = ''

    def make_redis_string(self):
        return f'{self.riot_id_name}/@#{self.riot_id_tag_line}/@#{self.summoner_id}/@#{self.account_id}/@#{self.puu_id}/@#{self.match_id}/@#{self.platform_id}/@#{self.api_type}'


class Challenge(BaseModel):
    """
    챌린지 정보

    challenge_id: 챌린지 id
    percentile: 진행도
    level: 레벨
    value: 값
    achieved_time: 달성 시점
    """
    challenge_id: int = 0
    percentile: float = 0
    level: str = ""
    value: float = 0


def get_summoner_api_status(platform_id: str) -> int:
    """
    riot에서 제공하는 tier api 상태를 확인하는 함수
    반환값이 0이면 정상, 1이면 이상
    :param platform_id: 지역
    :return 0 or 1:
    """
    conn = connect_sql_aurora(RDS_INSTANCE_TYPE.READ)
    try:
        query = f'select is_ok ' \
                f'from b2c_riot_api_status{conf_dict["TABLE_STR"]} ' \
                f'where platform_id = {repr(platform_id)} ' \
                f'and type = "tier"'
        status = sql_execute(query, conn)[0][0]
        return status
    except:
        return 1
    finally:
        conn.close()


def get_challenge_list(challenges_data: dict) -> list:
    '''
    challenge data를 입력받아 필요한 3개의 도전과제 데이터만 추출하는 함수
    :param challenges_data:
    :return:
    '''
    challenges_list = []
    for challenge in challenges_data.get("challenges"):
        if challenge.get("challengeId") in challenges_data.get("preferences")['challengeIds']:
            challenges_list.append(
                Challenge(
                    challenge_id=challenge.get("challengeId"),
                    percentile=challenge.get("percentile"),
                    level=challenge.get("level"),
                    value=challenge.get("value")
                )
            )
    for i in range(3):
        try:
            data = challenges_list[i]
        except:
            challenges_list.append(Challenge())
    return challenges_list


def get_ranked_win_loss(res: dict, ranked: str) -> tuple:
    """
    rank별 승/패 승격전 정보를 추출하는 함수
    :param res:
    :param ranked:
    :return:
    """
    wins, losses, mini_progress, mini_wins, mini_losses = 0, 0, "", 0, 0
    if res.get(ranked):
        ranked_dict = res.get(ranked)
        wins = ranked_dict.get("wins")
        losses = ranked_dict.get("losses")
        if ranked_dict.get("miniSeries"):
            mini_dict = ranked_dict.get("miniSeries")
            mini_progress = mini_dict.get("progress")
            mini_wins = mini_dict.get("wins")
            mini_losses = mini_dict.get("losses")
    return wins, losses, mini_progress, mini_wins, mini_losses


def insert_summoner_basic_info(res: dict, platform_id: str, ) -> bool:
    """
    소환사의 기본 정보를 DB에 입력 또는 업데이트하는 함수
    :param res:
    :param platform_id:
    :return:

    Args:
        riot_id_name:
        riot_id_tag_line:
    """
    # table_name_season = "_15" if season_over else ""
    conn = connect_sql_aurora(RDS_INSTANCE_TYPE.WRITE)
    try:
        riot_id_name = copy.deepcopy(res['riot_id_name']).replace('%20', '').replace(' ', '').replace("\xa0", "").lower()
        riot_id = f"{riot_id_name}#{res['riot_id_tag_line']}"
        wins, losses, mini_progress, mini_wins, mini_losses = get_ranked_win_loss(res=res, ranked="RANKED_SOLO_5x5")
        wins_flex, losses_flex, mini_progress_flex, mini_wins_flex, mini_losses_flex = \
            get_ranked_win_loss(res=res, ranked="RANKED_FLEX_SR")
        summoner_name = copy.deepcopy(res['name']).replace('%20', '').replace(' ', '').replace("\xa0", "").lower()
        tier = res.get("RANKED_SOLO_5x5")['tier'] if res.get("RANKED_SOLO_5x5") else 'unranked'
        division = res.get("RANKED_SOLO_5x5")['rank'] if res.get("RANKED_SOLO_5x5") else ''
        lp = res.get("RANKED_SOLO_5x5")['leaguePoints'] if res.get("RANKED_SOLO_5x5") else 0
        rank = tier if tier in ('CHALLENGER', "GRANDMASTER", "MASTER", "unranked") else f"{tier} {division}"
        tier_flex = res.get("RANKED_FLEX_SR")['tier'] if res.get("RANKED_FLEX_SR") else 'unranked'
        division_flex = res.get("RANKED_FLEX_SR")['rank'] if res.get("RANKED_FLEX_SR") else ''
        lp_flex = res.get("RANKED_FLEX_SR")['leaguePoints'] if res.get("RANKED_FLEX_SR") else 0
        rank_flex = tier_flex if tier_flex in (
            'CHALLENGER', "GRANDMASTER", "MASTER", "unranked") else f"{tier_flex} {division_flex}"

        utf_name = str(summoner_name.encode("utf-8"))
        origin_name = summoner_name if r'\xa0' in repr(riot_id) else res.get('name')

        challenges_data = res.get("challenges")
        try:
            challenges_title = int(challenges_data.get("preferences")['title'])
        except:
            challenges_title = 0
        try:
            challenge_list = get_challenge_list(challenges_data=challenges_data)
        except:
            challenge_list = []

        ### tier_batch 테이블(b2c_summoner_tier_history) 업데이트
        if tier == 'unranked' and tier_flex == 'unranked':
            pass
        else:
            reg_date = (round(round(datetime.now(timezone('Asia/Seoul')).timestamp()) / (3600 * 24)) * (3600 * 24))
            query = f'INSERT INTO ' \
                    f'b2c_summoner_tier_history_partitioned{conf_dict["TABLE_STR"]}(' \
                    f'summoner_id, platform_id, regdate, summoner_name, tier, lp, tier_flex, lp_flex, games, wins, games_flex, wins_flex) ' \
                    f'VALUES({repr(res.get("id"))}, {repr(platform_id)}, {reg_date}, {repr(origin_name)}, {repr(rank)}, ' \
                    f'{lp}, {repr(rank_flex)}, {lp_flex}, {wins + losses}, {wins}, {wins_flex + losses_flex}, {wins_flex}) ' \
                    f'ON DUPLICATE KEY UPDATE summoner_name = {repr(origin_name)}, tier = {repr(rank)}, lp = {lp}, ' \
                    f'tier_flex = {repr(rank_flex)}, lp_flex = {lp_flex}, games = {wins + losses}, wins = {wins}, ' \
                    f'games_flex = {wins_flex + losses_flex}, wins_flex = {wins_flex}'
            sql_execute(query, conn)
        ##

        query = f'INSERT INTO ' \
                f'b2c_summoner_basic_data_utf{conf_dict["TABLE_STR"]}(' \
                f'summoner_id, platform_id, summoner_name, summoner_name_utf, summoner_name_origin, wins, losses, ' \
                f'wins_flex, losses_flex, icon_id, puu_id, summoner_level, ' \
                f'mini_series_progress, mini_series_wins, mini_series_losses, ' \
                f'mini_series_progress_flex, mini_series_wins_flex, mini_series_losses_flex, ' \
                f'tier, lp, tier_flex, lp_flex, riot_id_name, riot_id_tag_line) ' \
                f'VALUES({repr(res.get("id"))}, {repr(platform_id)}, {repr(summoner_name)}, {repr(utf_name)}, ' \
                f'{repr(origin_name)}, ' \
                f'{wins}, {losses}, {wins_flex}, {losses_flex}, {res.get("profileIconId")}, {repr(res.get("puuid"))}, ' \
                f'{res.get("summonerLevel")}, {repr(mini_progress)}, {mini_wins}, {mini_losses}, ' \
                f'{repr(mini_progress_flex)}, {mini_wins_flex}, {mini_losses_flex}, ' \
                f'{repr(rank)}, {lp}, {repr(rank_flex)}, {lp_flex}, {repr(riot_id_name)}, {repr(res["riot_id_tag_line"])}) ' \
                f'ON DUPLICATE KEY UPDATE summoner_name = {repr(summoner_name)}, summoner_name_utf = {repr(utf_name)}, ' \
                f'summoner_name_origin = {repr(origin_name)}, ' \
                f'wins = {wins}, losses = {losses}, ' \
                f'wins_flex = {wins_flex}, losses_flex = {losses_flex}, icon_id = {res.get("profileIconId")}, ' \
                f'puu_id = {repr(res.get("puuid"))}, summoner_level = {res.get("summonerLevel")}, ' \
                f'mini_series_progress = {repr(mini_progress)}, mini_series_wins = {mini_wins}, mini_series_losses = {mini_losses}, ' \
                f'mini_series_progress_flex = {repr(mini_progress_flex)}, mini_series_wins_flex = {mini_wins_flex}, ' \
                f'mini_series_losses_flex = {mini_losses_flex}, ' \
                f'tier = {repr(rank)}, lp = {lp}, tier_flex = {repr(rank_flex)}, lp_flex = {lp_flex}, ' \
                f'riot_id_name = {repr(riot_id_name)}, riot_id_tag_line = {repr(res["riot_id_tag_line"])}'

        sql_execute(query, conn)

        def change_null_data(data_list, index):
            '''
            빈 리스트 데이터의 경우 기본값을 반환
            :param data_list:
            :param index:
            :return:
            '''
            try:
                return data_list[index]
            except:
                return Challenge()

        query = f'INSERT INTO ' \
                f'b2c_summoner_challenges_data{conf_dict["TABLE_STR"]}(' \
                f'summoner_id, platform_id, title_id, challenge_id_1, percentile_1, level_1, value_1, ' \
                f'challenge_id_2, percentile_2, level_2, value_2, challenge_id_3, percentile_3, level_3, value_3) ' \
                f'VALUES({repr(res.get("id"))}, {repr(platform_id)}, {challenges_title}, ' \
                f'{change_null_data(challenge_list, 0).challenge_id}, {change_null_data(challenge_list, 0).percentile}, {repr(change_null_data(challenge_list, 0).level)}, {change_null_data(challenge_list, 0).value}, ' \
                f'{change_null_data(challenge_list, 1).challenge_id}, {change_null_data(challenge_list, 1).percentile}, {repr(change_null_data(challenge_list, 1).level)}, {change_null_data(challenge_list, 1).value}, ' \
                f'{change_null_data(challenge_list, 2).challenge_id}, {change_null_data(challenge_list, 2).percentile}, {repr(change_null_data(challenge_list, 2).level)}, {change_null_data(challenge_list, 2).value}) ' \
                f'ON DUPLICATE KEY UPDATE title_id={challenges_title}, ' \
                f'challenge_id_1={change_null_data(challenge_list, 0).challenge_id}, percentile_1={change_null_data(challenge_list, 0).percentile}, ' \
                f'level_1={repr(change_null_data(challenge_list, 0).level)}, value_1={change_null_data(challenge_list, 0).value}, ' \
                f'challenge_id_2={change_null_data(challenge_list, 1).challenge_id}, percentile_2={change_null_data(challenge_list, 1).percentile}, ' \
                f'level_2={repr(change_null_data(challenge_list, 1).level)}, value_2={change_null_data(challenge_list, 1).value}, ' \
                f'challenge_id_3={change_null_data(challenge_list, 2).challenge_id}, percentile_3={change_null_data(challenge_list, 2).percentile}, ' \
                f'level_3={repr(change_null_data(challenge_list, 2).level)}, value_3={change_null_data(challenge_list, 2).value}'
        sql_execute(query, conn)

        conn.commit()
        print(f'{get_current_datetime()} | - Data Inserted')
        return True
    except Exception as e:
        pass
    finally:
        conn.close()


def get_summoner_api_url(current_obj: ApiInfo):
    summoner = RiotV4Summoner(api_key=riot_api_key, platform_id=current_obj.platform_id)

    if current_obj.summoner_id:
        return summoner.get_url(summoner_id=current_obj.summoner_id)
    elif current_obj.puu_id:
        return summoner.get_url(puu_id=current_obj.puu_id)
    else:
        raise Exception('summoner_API: No summoner_info')


def get_tier_api_url(current_obj: ApiInfo):
    tier = RiotV4Tier(api_key=riot_api_key, platform_id=current_obj.platform_id)
    return tier.get_by_summoner(summoner_id=current_obj.summoner_id)


def get_challenge_api_url(current_obj: ApiInfo):
    challenge = RiotV1Challenges(api_key=riot_api_key, platform_id=current_obj.platform_id)
    return challenge.get_url(puu_id=current_obj.puu_id)

def get_account_api_url(current_obj: ApiInfo):
    account = RiotV1Accounts(api_key=riot_api_key)
    return account.get_url_by_puu_id(puu_id=current_obj.puu_id)


def is_api_status_green(result):
    return result.status_code == 200

def is_unsearchable_response(result):
    return result.status_code == 400 or result.status_code == 403 or result.status_code == 404 or result.status_code == 401 or result.status_code == 503

def get_current_waiting_object() -> ApiInfo:
    r = rd.rpop('error_list')
    current_obj = ApiInfo(*r.split('/@#'))
    print(f'{get_current_datetime()} | ',current_obj, rd.llen('error_list'))

    return current_obj

def get_current_datetime():
    return datetime.now() + timedelta(hours=9)


def queue_system():
    is_queue_is_empty_string_not_printed = True

    while True:
        # 대기열 비어있는 경우 시스템 대기
        try:
            if rd.llen('error_list') == 0 and is_queue_is_empty_string_not_printed:
                print(f'{get_current_datetime()} | Queue is Empty')
                print('------------------------------\n')
                is_queue_is_empty_string_not_printed = False

            # 대기열 인원 체크
            elif rd.llen('error_list') >= 1:
                is_queue_is_empty_string_not_printed = True

                current_obj = get_current_waiting_object()

                summoner_result = get_json_time_limit(
                    get_summoner_api_url(current_obj),
                    time_limit=10
                )
                if is_api_status_green(summoner_result):
                    summoner = summoner_result.json()
                    current_obj.puu_id = summoner['puuid']
                    current_obj.summoner_id = summoner['id']
                    current_obj.summoner_name = summoner['name']
                    current_obj.account_id = summoner['accountId']

                elif is_unsearchable_response(summoner_result):
                    print(f"{get_current_datetime()} | {summoner_result.json()['status']['message']}")
                    print('------------------------------\n')
                    continue
                else:
                    print(f"{get_current_datetime()} | ",summoner_result.json())
                    rd.rpush('error_list', current_obj.make_redis_string())
                    system_sleep(retry_after=get_max_retry_after(summoner_result))


                tier_result = get_json_time_limit(
                    get_tier_api_url(current_obj),
                    time_limit=10
                )

                challenge_result = get_json_time_limit(
                    get_challenge_api_url(current_obj),
                    time_limit=10
                )
                account_result = get_json_time_limit(
                    get_account_api_url(current_obj),
                    time_limit=10
                )
                if current_obj.platform_id in SERVER_NOT_WORKING:
                    print('SERVER_NOT_WORKING')
                    print(f'current server: {current_obj.platform_id}')
                    print(f'{tier_result.json()}')

                elif is_api_status_all_green(challenge_result, summoner_result, tier_result):
                    res = make_res(challenge_result, summoner_result, tier_result, account_result)
                    if res is None:
                        print('No Account v1 Response  (gameName, tagLine)')
                    else:
                        insert_summoner_basic_info(res=res, platform_id=current_obj.platform_id)
                else:
                    rd.rpush('error_list', current_obj.make_redis_string())
                    system_sleep(retry_after=get_max_retry_after(summoner_result, tier_result, challenge_result))

                # 현재 대기인원
                print('------------------------------\n')
        except Exception as e:
            print(traceback.format_exc())

def make_res(challenge_result, summoner_result, tier_result, account_result):
    res = summoner_result.json()
    res['challenges'] = challenge_result.json()
    for tier_info in tier_result.json():
        if tier_info['queueType'] == 'RANKED_SOLO_5x5':
            res['RANKED_SOLO_5x5'] = tier_info
        elif tier_info['queueType'] == 'RANKED_FLEX_SR':
            res['RANKED_FLEX_SR'] = tier_info
    account_json = account_result.json()
    if account_json.get('gameName') is None and account_json.get('tagLine') is None:
        return None
    res['riot_id_name'] = account_json.get('gameName')
    res['riot_id_tag_line'] = account_json.get('tagLine')
    return res


def get_max_retry_after(summoner_result={}, tier_result={}, challenge_result={}):
    total_retry_after = [0]

    if isinstance(summoner_result, requests.models.Response):
        summoner_retry_after = int(summoner_result.headers.get('Retry-After') if summoner_result.headers.get('Retry-After') else 0)
        total_retry_after.append(summoner_retry_after)

    if isinstance(tier_result, requests.models.Response):
        tier_retry_after = int(tier_result.headers.get('Retry-After') if tier_result.headers.get('Retry-After') else 0)
        total_retry_after.append(tier_retry_after)

    if isinstance(challenge_result, requests.models.Response):
        challenge_retry_after = int(challenge_result.headers.get('Retry-After') if challenge_result.headers.get('Retry-After') else 0)
        total_retry_after.append(challenge_retry_after)

    return max(total_retry_after)


def is_api_status_all_green(challenge_result, summoner_result, tier_result):
    return is_api_status_green(summoner_result) and is_api_status_green(tier_result) and is_api_status_green(
        challenge_result)


def system_sleep(retry_after):
    print(f"{get_current_datetime()} | Because of API Limit, it will restart in {retry_after}s")
    time.sleep(retry_after)


def run():
    waiting_redis_init()

    print('Message Queue System Init')
    print('-- Done\n')

    print('Run Start\n\n')
    queue_system()


def waiting_redis_init(waiting_sec=20):
    print('Waiting Redis Init')

    for _ in range(waiting_sec):
        print(_, end='\r')
        time.sleep(1)

    print('')
    print('-- Done\n')





if __name__ == '__main__':
    try:
        run()
    except Exception as e:
        print(e)




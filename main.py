import copy
import time

from pydantic import BaseModel
from datetime import datetime
from db import sql_execute, connect_sql_aurora, conf_dict
import redis


def get_summoner_api_status(platform_id: str) -> int:
    """
    riot에서 제공하는 summoner api 상태를 확인하는 함수
    반환값이 0이면 정상, 1이면 이상
    :param platform_id: 지역
    :return 0 or 1:
    """
    conn = connect_sql_aurora()
    try:
        query = f'select is_ok from b2c_riot_api_status{conf_dict["TABLE_STR"]} ' \
                f'where platform_id = {repr(platform_id)} and type = "summoner"'
        status = sql_execute(query, conn)[0][0]
        return status
    except:
        return 1
    finally:
        conn.close()

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

def get_tier_history_table_name(month):
    if month in [1, 4, 7, 10]:
        return 'b2c_summoner_tier_history_1'
    if month in [2, 5, 8, 11]:
        return 'b2c_summoner_tier_history_2'
    if month in [3, 6, 9, 12]:
        return 'b2c_summoner_tier_history_3'

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

def insert_summoner_basic_info(res: dict, platform_id: str) -> bool:
    """
    소환사의 기본 정보를 DB에 입력 또는 업데이트하는 함수
    :param res:
    :param platform_id:
    :return:
    """
    # season_over = get_season_over(platform_id=platform_id)
    # table_name_season = "_15" if season_over else ""
    conn = connect_sql_aurora()
    try:
        wins, losses, mini_progress, mini_wins, mini_losses = get_ranked_win_loss(res=res, ranked="RANKED_SOLO_5x5")
        wins_flex, losses_flex, mini_progress_flex, mini_wins_flex, mini_losses_flex = \
            get_ranked_win_loss(res=res, ranked="RANKED_FLEX_SR")
        summoner_name = copy.deepcopy(res.get("name")).replace('%20', '').replace(' ', '').replace("\xa0", "").lower()
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
        origin_name = summoner_name if r'\xa0' in repr(res.get("name")) else res.get("name")
        challenges_data = res.get("challenges")
        try:
            challenges_title = int(challenges_data.get("preferences")['title'])
        except:
            challenges_title = 0
        try:
            challenge_list = get_challenge_list(challenges_data=challenges_data)
        except:
            challenge_list = []

        ### tier_batch 테이블(b2c_summoner_tier_history_x) 업데이트
        reg_date = int(datetime.strptime(datetime.today().strftime('%Y-%m-%d 09:00:00'), '%Y-%m-%d %H:%M:%S').timestamp() - 32400)
        query = f'INSERT INTO ' \
                f'{get_tier_history_table_name(datetime.today().month)}(' \
                f'summoner_id, platform_id, regdate, summoner_name, tier, lp, tier_flex, lp_flex, games, wins, games_flex, wins_flex) ' \
                f'VALUES({repr(res.get("id"))}, {repr(platform_id)}, {reg_date}, {repr(origin_name)}, {repr(rank)}, '\
                f'{lp}, {repr(rank_flex)}, {lp_flex}, {wins + losses}, {wins}, {wins_flex + losses_flex}, {wins_flex}) ' \
                f'ON DUPLICATE KEY UPDATE summoner_name = {repr(origin_name)}, tier = {repr(rank)}, lp = {lp}, '\
                f'tier_flex = {repr(rank_flex)}, lp_flex = {lp_flex}, games = {wins + losses}, wins = {wins}, '\
                f'games_flex = {wins_flex + losses_flex}, wins_flex = {wins_flex}'

        sql_execute(query, conn)
        ###

        query = f'INSERT INTO ' \
                f'b2c_summoner_basic_data_utf(' \
                f'summoner_id, platform_id, summoner_name, summoner_name_utf, summoner_name_origin, wins, losses, ' \
                f'wins_flex, losses_flex, icon_id, puu_id, summoner_level, ' \
                f'mini_series_progress, mini_series_wins, mini_series_losses, ' \
                f'mini_series_progress_flex, mini_series_wins_flex, mini_series_losses_flex, ' \
                f'tier, lp, tier_flex, lp_flex) ' \
                f'VALUES({repr(res.get("id"))}, {repr(platform_id)}, {repr(summoner_name)}, {repr(utf_name)}, ' \
                f'{repr(origin_name)}, ' \
                f'{wins}, {losses}, {wins_flex}, {losses_flex}, {res.get("profileIconId")}, {repr(res.get("puuid"))}, ' \
                f'{res.get("summonerLevel")}, {repr(mini_progress)}, {mini_wins}, {mini_losses}, ' \
                f'{repr(mini_progress_flex)}, {mini_wins_flex}, {mini_losses_flex}, ' \
                f'{repr(rank)}, {lp}, {repr(rank_flex)}, {lp_flex}) ' \
                f'ON DUPLICATE KEY UPDATE summoner_name = {repr(summoner_name)}, summoner_name_utf = {repr(utf_name)}, ' \
                f'summoner_name_origin = {repr(origin_name)}, ' \
                f'wins = {wins}, losses = {losses}, ' \
                f'wins_flex = {wins_flex}, losses_flex = {losses_flex}, icon_id = {res.get("profileIconId")}, ' \
                f'puu_id = {repr(res.get("puuid"))}, summoner_level = {res.get("summonerLevel")}, ' \
                f'mini_series_progress = {repr(mini_progress)}, mini_series_wins = {mini_wins}, mini_series_losses = {mini_losses}, ' \
                f'mini_series_progress_flex = {repr(mini_progress_flex)}, mini_series_wins_flex = {mini_wins_flex}, ' \
                f'mini_series_losses_flex = {mini_losses_flex}, ' \
                f'tier = {repr(rank)}, lp = {lp}, tier_flex = {repr(rank_flex)}, lp_flex = {lp_flex}'

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
                f'b2c_summoner_challenges_data(' \
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
        return True
    except Exception as e:
        pass
    finally:
        conn.close()


def main():
    # 대기열 인원 체크

    # 라이엇 API 상태 체크
    # 처리 가능 시
        # API 요청
        # 반환 정보 DB 업데이트

    # 처리 불가능 시
        # 대기


    try:
        rd = redis.Redis(host='host.docker.internal', port=6379, decode_responses=True)
        rd.set('hi', 'hello')
        rd.set('test', 'ok')
    except Exception as e:
        with open('error.log','w') as f:
            f.write(str(e.args))
            f.close()


    while True:
        with open('stdio.log','w') as f:
            f.write(f"Hello World, {rd.get(name='hi')}, {rd.get(name='test')}")
            f.close()

        time.sleep(10)


if __name__ == '__main__':
    main()
# import copy
# import os
# import time
import traceback
from datetime import datetime, timedelta
from common.db import sql_execute, connect_sql_aurora, conf_dict, riot_api_key, RDS_INSTANCE_TYPE
# from common.riot import get_json_time_limit, RiotV4Tier, RiotV4Summoner, RiotV1Accounts, RiotV1Challenges
# from enum import Enum, auto
# from dataclasses import dataclass




def get_current_datetime():
    return datetime.now() + timedelta(hours=9)


def queue_system():
    '''
    TODO:
        Match API 분당 최대 개수 파악

    TODO:
        While
            if Summoner Queue Table에 대기 상태 존재하는 경우:
                (대기 상태인 소환사 한명 호출)
                Riot API에서 해당 소환사 시즌 전체 match_id 수집
                입력된 통계 데이터, 대기 목록에 중복되는 match_id 제거
                                                                    _
                if 처리할 데이터가 없는 경우:
                  Summoner Queue Table 해당 소환사 Status 완료로 표시
                  continue
                                                                    _
                처리 필요한 Match_id들 Match Queue Table에 대기로 insert(bulk insert)
                해당 소환사 Summoner Queue Table에 진행상태로 update
                                                                    _
            elif Summoner Queue Table에 대기 상태 없고 진행 상태만 있는 경우:
                (진행 상태인 소환사 호출)
                if Match table에서 해당 소환사의 match들 전부 완료인 경우:
                  Summoner Queue Table 해당 소환사 Status 완료로 표시
                                                                    _
            elif Summoner Queue Table 대기 상태 없음, 진행상태 없음:
                현재 상태 대기
    '''
    while True:
        try:
            conn = connect_sql_aurora(RDS_INSTANCE_TYPE.READ)
            r = sql_execute(
                'SELECT * '
                'from b2c_summoner_queue',
                conn
            )
            print(r)

        except Exception as e:
            print(traceback.format_exc())
        finally:
            conn.close()

def run():
    pass


if __name__ == '__main__':
    try:
        queue_system()
    except Exception as e:
        print(e)




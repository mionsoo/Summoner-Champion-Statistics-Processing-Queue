import asyncio
import time
import traceback

import sys
sys.path.append("/usr/src/app")

from common.utils import get_current_datetime
from core.stat_summoner_queue import SummonerQueueOperator
from core.stat_queue_sys import QueueComment


async def queue_system():
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
    '''
    queue_comment = QueueComment()
    queue_op = SummonerQueueOperator()

    while True:
        try:
            await queue_op.update_new_data()

            if queue_op.is_all_queue_is_empty() and queue_comment.is_need_to_print_empty():
                print(f'{get_current_datetime()} | Queue is Empty')
                print('------------------------------\n')
                queue_comment.empty_printed()
                await asyncio.sleep(60)

            elif queue_op.is_data_exists():
                current_objs = await queue_op.get_current_obj(3)
                if current_objs is not None:
                    tasks = [asyncio.create_task(queue_op.process_job(current_obj)) for current_obj in current_objs]
                    await asyncio.gather(*tasks)
                    queue_op.print_remain()
                    print('------------------------------\n')

                queue_comment.print_empty()

        except Exception:
            print(traceback.format_exc())


if __name__ == '__main__':
    try:
        asyncio.run(queue_system())
    except Exception as e:
        print(e)

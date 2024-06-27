import asyncio
import traceback

import sys
sys.path.append("/usr/src/app")

from common.db import connect_sql_aurora_async, execute_update_queries_summoner_wait, RDS_INSTANCE_TYPE, execute_update_queries_summoner
from common.const import S_EXECUTE_SUMMONER_COUNT, Status
from common.utils import get_current_datetime
from core.stat_summoner_queue import SummonerQueueOperator
from core.stat_queue_sys import QueueEmptyComment



async def queue_system():
    """
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
    """
    queue_empty_comment = QueueEmptyComment()
    queue_op = SummonerQueueOperator()
    conn = await connect_sql_aurora_async(RDS_INSTANCE_TYPE.READ)

    while True:
        try:
            await queue_op.update_new_data(conn)

            if queue_op.is_all_queue_is_empty() and queue_empty_comment.is_set_print():
                print(f'{get_current_datetime()} | Queue is Empty')
                print('------------------------------\n')

                queue_empty_comment.set_printed()

            elif queue_op.is_all_queue_is_empty():
                print('sleep')
                await asyncio.sleep(20)

            elif queue_op.is_data_exists():
                current_objs = await queue_op.get_current_obj(S_EXECUTE_SUMMONER_COUNT)
                if current_objs is None:
                    continue

                if current_objs[0].status == Status.Waiting.code:
                    tasks = [asyncio.create_task(queue_op.process_job(current_obj, conn)) for current_obj in current_objs]
                    return_data = await asyncio.gather(*tasks)
                    tasks = [await execute_update_queries_summoner_wait(conn, data) for data in return_data]

                elif current_objs[0].status == Status.Working.code:
                    return_datas = []
                    skip_thld = 20
                    for start_idx in range(0, len(current_objs), skip_thld):
                        end_idx = start_idx + skip_thld
                        tasks = [asyncio.create_task(queue_op.process_job(current_obj, conn)) for current_obj in current_objs[start_idx:end_idx]]
                        return_data = await asyncio.gather(*tasks)
                        return_datas.extend(return_data)


                    tasks = await execute_update_queries_summoner(conn, return_datas)

                queue_op.print_counts_remain()
                print('------------------------------\n')

                queue_empty_comment.set_print()

        except Exception:
            print("tt ",traceback.format_exc())


if __name__ == '__main__':
    try:
        # loop = asyncio.get_event_loop()
        asyncio.run(queue_system())
        # loop.run_until_complete(queue_system(loop))
    except Exception as e:
        print(e)

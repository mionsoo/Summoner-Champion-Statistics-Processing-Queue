import asyncio
import traceback

import sys
sys.path.append("/usr/src/app")

from common.db import connect_sql_aurora_async, execute_update_queries_summoner_wait, RDS_INSTANCE_TYPE, execute_update_queries_summoner
from common.const import S_EXECUTE_SUMMONER_COUNT, Status
from core.Queue.stat_summoner_queue import SummonerQueueOperator
from core.Queue.stat_queue_sys import QueueEmptyComment
from core.Job.stat_summoner_job import StatQueueSummonerJob

async def run_queue(queue_op, conn):
    current_objs = await queue_op.get_current_obj(S_EXECUTE_SUMMONER_COUNT)

    if current_objs is None:
        return 0

    if current_objs[0].status == Status.Waiting.code:
        tasks = [asyncio.create_task(StatQueueSummonerJob(current_obj).process()) for current_obj in current_objs]
        job_results = await asyncio.gather(*tasks)
        tasks = [await execute_update_queries_summoner_wait(conn, job_result) for job_result in job_results]

    elif current_objs[0].status == Status.Working.code:
        job_results = []
        skip_thld = 20
        for start_idx in range(0, len(current_objs), skip_thld):
            end_idx = start_idx + skip_thld
            tasks = [asyncio.create_task(StatQueueSummonerJob(current_obj).process()) for current_obj in current_objs[start_idx:end_idx]]
            return_data = await asyncio.gather(*tasks)
            job_results.extend(return_data)
        tasks = await execute_update_queries_summoner(conn, job_results)


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

            if queue_op.is_all_job_done() and queue_empty_comment.is_set_print():
                await queue_empty_comment.print_job_ended()

            elif queue_op.is_all_job_done():
                await queue_op.sleep_queue()

            elif queue_op.is_data_exists():
                await run_queue(queue_op, conn)
                queue_op.print_counts_remain()
                queue_empty_comment.set_job_not_done()
                print('------------------------------\n')

        except Exception:
            print("tt ",traceback.format_exc())


if __name__ == '__main__':
    try:
        asyncio.run(queue_system())
    except Exception as e:
        print(e)

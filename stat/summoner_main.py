import asyncio
import sys
import traceback

sys.path.append("/usr/src/app")

from common.const import S_EXECUTE_SUMMONER_COUNT, Status
from common.db import (
    RDS_INSTANCE_TYPE,
    connect_sql_aurora_async,
    execute_update_queries_summoner,
    execute_update_queries_summoner_wait,
)
from core.Job.stat_summoner_job import StatQueueSummonerJob
from core.Queue.stat_queue_sys import QueueEmptyComment
from core.Queue.stat_summoner_queue import SummonerQueueOperator


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
        skip_thld = 50
        for start_idx in range(0, len(current_objs), skip_thld):
            end_idx = start_idx + skip_thld
            tasks = [
                asyncio.create_task(StatQueueSummonerJob(current_obj).process())
                for current_obj in current_objs[start_idx:end_idx]
            ]
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
    sys_log = QueueEmptyComment()
    sys_oper = SummonerQueueOperator()
    conn = await connect_sql_aurora_async(RDS_INSTANCE_TYPE.READ)

    while True:
        if conn.closed:
            conn = await connect_sql_aurora_async(RDS_INSTANCE_TYPE.READ)

        try:
            await sys_oper.update_incoming_data(conn)
            sys_oper.print_counts_remain()

            if sys_oper.is_all_job_done() and sys_log.is_empty_log_not_printed():
                await sys_log.print_empty_log()

            elif sys_oper.is_all_job_done():
                await sys_oper.sleep_queue()

            elif sys_oper.is_data_exists():
                sys_log.set_empty_log_not_printed()
                await sys_oper.check_burst_switch_on_off()
                await run_queue(sys_oper, conn)
                await sys_oper.check_burst_switch_on_off()
                sys_oper.print_counts_remain()
                print("------------------------------\n")

        except Exception:
            print("tt ", traceback.format_exc())


if __name__ == "__main__":
    try:
        asyncio.run(queue_system())
    except Exception as e:
        print(e)

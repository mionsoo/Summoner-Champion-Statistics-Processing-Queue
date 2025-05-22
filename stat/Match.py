import asyncio
import sys
import traceback
from itertools import chain

sys.path.append("/usr/src/app")

from common.const import MATCH_QUEUE_THROUGHPUT, JobStatus
from common.db import (
    RDS_INSTANCE_TYPE,
    connect_sql_aurora_async,
    execute_match_insert_queries,
    execute_matches,
    update_current_job_status,
)
from core.Job.stat_match_job import StatQueueMatchJob
from core.Queue.Match import MatchOperator
from core.Queue.System import Comments


async def run_queue(sys_oper, conn):
    current_objs = await sys_oper.get_current_job(MATCH_QUEUE_THROUGHPUT)
    if None in current_objs:
        return 0

    tasks = []
    async with conn.cursor() as cursor:
        for current_obj in current_objs:
            match_ids = await execute_matches(current_obj, cursor)
            job = StatQueueMatchJob(current_obj=current_obj)
            tasks.append(asyncio.create_task(job.process(match_ids)))

    job_results = await asyncio.gather(*tasks)
    queries = [job_result.data for job_result in job_results]
    t_queries = sum(chain.from_iterable(queries), [])

    await execute_match_insert_queries(conn, t_queries)
    await update_current_job_status(conn, current_objs, t_queries)
    _ = [await sys_oper.return_to_queue(job_result) for job_result in job_results
         if job_result.processed_status == JobStatus.Error.type]


async def main():
    sys_log = Comments()
    sys_oper = MatchOperator()
    conn = await connect_sql_aurora_async(RDS_INSTANCE_TYPE.READ)

    while True:
        if conn.closed:
            break

        try:
            await sys_oper.update_incoming_data(conn)
            await sys_oper.print_remain_counts(conn)

            if sys_oper.is_all_job_done() and sys_log.is_empty_log_not_printed():
                await sys_log.print_empty_log()

            elif sys_oper.is_all_job_done():
                await sys_oper.sleep_queue()

            elif sys_oper.is_job_exists():
                sys_log.set_empty_log_not_printed()
                await run_queue(sys_oper, conn)
                await sys_oper.print_remain_counts(conn)
                print("------------------------------\n")

        except Exception:
            print(traceback.format_exc())


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(e)

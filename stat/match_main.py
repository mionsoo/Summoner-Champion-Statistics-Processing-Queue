import traceback
from itertools import chain

import sys

from helper.queries import execute_matches

sys.path.append("/usr/src/app")
from common.const import M_EXECUTE_SUMMONER_COUNT, Status
from common.db import connect_sql_aurora_async, RDS_INSTANCE_TYPE, execute_match_insert_queries, update_current_obj_status
from core.stat_match_queue import SummonerMatchQueueOperator
from core.stat_queue_sys import QueueEmptyComment
from core.stat_match_job import StatQueueMatchJob
import asyncio


async def run_queue(queue_op, conn):
        current_objs = await queue_op.get_current_obj(M_EXECUTE_SUMMONER_COUNT)
        if None in current_objs:
            return 0

        tasks = []
        async with conn.cursor() as cursor:
            for idx, current_obj in enumerate(current_objs):
                match_ids = await execute_matches(current_obj, cursor)
                job = StatQueueMatchJob(current_obj=current_obj)
                tasks.append(asyncio.create_task(job.process(match_ids)))

        job_results = await asyncio.gather(*tasks)
        queries = [job_result.data for job_result in job_results]
        t_queries = sum(chain.from_iterable(queries), [])

        await execute_match_insert_queries(conn, t_queries)
        await update_current_obj_status(conn, current_objs, t_queries)
        await queue_op.print_counts_remain(conn)
        _ = [await queue_op.go_back_to_queue(job_result) for job_result in job_results if job_result.result_status == Status.Error.code]


async def main():
    queue_comment = QueueEmptyComment()
    queue_op = SummonerMatchQueueOperator()
    conn = await connect_sql_aurora_async(RDS_INSTANCE_TYPE.READ)

    while True:
        try:
            await queue_op.update_new_data(conn)

            if queue_op.is_all_job_done() and queue_comment.is_set_print():
                await queue_comment.print_job_ended()

            elif queue_op.is_all_job_done():
                await queue_op.sleep_queue()

            elif queue_op.is_data_exists():
                await run_queue(queue_op, conn)
                queue_comment.set_job_not_done()
                print('------------------------------\n')


        except Exception:
            print(traceback.format_exc())


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except Exception as e:
        print(e)

